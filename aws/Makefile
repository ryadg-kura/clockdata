# ClockData on AWS - run `make` to list the targets.
#
# Credentials are never stored here: Terraform, the AWS CLI and boto3 all use
# the standard AWS chain (AWS_PROFILE / SSO / environment variables).

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

TF      ?= terraform
TF_DIR  := infra
VENV    := .venv
PY      := $(VENV)/bin/python
STAMP   := $(VENV)/.installed
TFOUT   := $(TF) -chdir=$(TF_DIR) output -raw

# Simulator knobs: make simulate DEVICES=10 DURATION=600 ANOMALY_RATE=0.1
DEVICES      ?= 5
INTERVAL     ?= 1
DURATION     ?= 300
ANOMALY_RATE ?= 0.02
INVALID_RATE ?= 0.01
# Aggregator window: make aggregate HOURS=24
HOURS        ?= 2
# Query selection: make query Q=queries/03_abnormal_heart_rate.sql
Q            ?=
# Region/project used by `make leftovers` once the Terraform state is gone
REGION         ?=
DEFAULT_REGION ?= eu-west-3
PROJECT        ?= clockdata
# Skip Terraform's "yes" prompt: make destroy AUTO_APPROVE=1
AUTO_APPROVE ?= 0
APPROVE      := $(if $(filter 1,$(AUTO_APPROVE)),-auto-approve,)

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- local only

# A stamp file (not the python binary, which is a symlink) tracks installs.
$(STAMP): requirements-dev.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --quiet --upgrade pip
	$(VENV)/bin/pip install --quiet -r requirements-dev.txt
	@touch $(STAMP)

.PHONY: venv
venv: $(STAMP) ## Create the local Python virtualenv (.venv)

.PHONY: test
test: $(STAMP) ## Run the unit + local end-to-end tests (no AWS needed)
	$(PY) -m pytest

.PHONY: simulate-dry
simulate-dry: $(STAMP) ## Print 10 ticks of simulated events, no AWS call
	$(PY) simulator/simulator.py --dry-run --count 10 --devices $(DEVICES)

.PHONY: fmt
fmt: ## Format the Terraform code
	$(TF) -chdir=$(TF_DIR) fmt -recursive

.PHONY: validate
validate: ## terraform fmt -check + validate (no AWS needed)
	$(TF) -chdir=$(TF_DIR) fmt -check -recursive
	$(TF) -chdir=$(TF_DIR) init -input=false -backend=false >/dev/null
	$(TF) -chdir=$(TF_DIR) validate

.PHONY: plan-offline
plan-offline: ## terraform plan with dummy credentials (no AWS account needed)
	$(TF) -chdir=$(TF_DIR) init -input=false >/dev/null
	env -u AWS_PROFILE AWS_ACCESS_KEY_ID=offline AWS_SECRET_ACCESS_KEY=offline \
	    AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_EC2_METADATA_DISABLED=true \
	    $(TF) -chdir=$(TF_DIR) plan -input=false -lock=false -refresh=false -var offline_plan=true

# ---------------------------------------------------------------- AWS

.PHONY: check-aws
check-aws: ## Check tools + show which AWS account/identity will be used
	@command -v $(TF) >/dev/null || { echo "terraform not found: see README 'Prerequisites'"; exit 1; }
	@command -v aws >/dev/null   || { echo "aws CLI not found: see README 'Prerequisites'"; exit 1; }
	@echo "Using AWS identity:"
	@aws sts get-caller-identity --output table || { \
	    echo "No valid AWS credentials. Run 'aws sso login --profile <name>' and 'export AWS_PROFILE=<name>'."; exit 1; }

.PHONY: init
init:
	$(TF) -chdir=$(TF_DIR) init -input=false

.PHONY: plan
plan: check-aws init ## terraform plan against your AWS account (creates nothing)
	$(TF) -chdir=$(TF_DIR) plan

.PHONY: deploy
deploy: check-aws init ## Create the whole stack (asks for confirmation) - BILLED HOURLY
	@echo ""
	@echo "!! The Kinesis shard is billed every hour while it exists (~\$$0.018/h in eu-west-3)."
	@echo "!! Run 'make destroy' as soon as your demo is over."
	@echo ""
	$(TF) -chdir=$(TF_DIR) apply $(APPROVE)
	@echo ""
	@$(TF) -chdir=$(TF_DIR) output
	@echo ""
	@echo "Next: make simulate  ->  make aggregate  ->  make query      (then: make destroy)"

.PHONY: simulate
simulate: $(STAMP) ## Send simulated watch events to Kinesis (DEVICES, INTERVAL, DURATION, ANOMALY_RATE)
	@STREAM=$$($(TFOUT) stream_name 2>/dev/null) || { echo "Stack not deployed: run 'make deploy' first."; exit 1; }; \
	REGION=$$($(TFOUT) region); \
	$(PY) simulator/simulator.py --stream "$$STREAM" --region "$$REGION" \
	    --devices $(DEVICES) --interval $(INTERVAL) --duration $(DURATION) \
	    --anomaly-rate $(ANOMALY_RATE) --invalid-rate $(INVALID_RATE)

.PHONY: aggregate
aggregate: ## Run the Silver -> Gold aggregation now (it also runs every 15 min)
	@FN=$$($(TFOUT) aggregator_function 2>/dev/null) || { echo "Stack not deployed: run 'make deploy' first."; exit 1; }; \
	OUT=$$(mktemp); trap 'rm -f "$$OUT"' EXIT; \
	aws lambda invoke --region "$$($(TFOUT) region)" --function-name "$$FN" \
	    --cli-binary-format raw-in-base64-out --payload '{"hours_back": $(HOURS)}' "$$OUT" >/dev/null; \
	python3 -m json.tool "$$OUT"

.PHONY: query
query: $(STAMP) ## Run the sample Athena queries (or one: Q=queries/03_abnormal_heart_rate.sql)
	@WG=$$($(TFOUT) athena_workgroup 2>/dev/null) || { echo "Stack not deployed: run 'make deploy' first."; exit 1; }; \
	$(PY) scripts/query.py --workgroup "$$WG" --database "$$($(TFOUT) glue_database)" \
	    --region "$$($(TFOUT) region)" $(Q)

.PHONY: dlq
dlq: ## Peek at the dead-letter queue (failed batches)
	@aws sqs receive-message --region "$$($(TFOUT) region)" --queue-url "$$($(TFOUT) dlq_url)" \
	    --max-number-of-messages 10 --visibility-timeout 0 --output json

.PHONY: urls
urls: ## Print the CloudWatch dashboard and Athena console URLs
	@echo "Dashboard: $$($(TFOUT) dashboard_url)"
	@echo "Athena:    $$($(TFOUT) athena_console_url)"

.PHONY: destroy
destroy: check-aws init ## Delete EVERY resource, including the S3 buckets and their data
	@REGION=$$($(TFOUT) region 2>/dev/null || true); \
	$(TF) -chdir=$(TF_DIR) destroy $(APPROVE); \
	$(MAKE) --no-print-directory leftovers $${REGION:+REGION=$$REGION}

.PHONY: leftovers
leftovers: ## List anything still tagged Project=clockdata (should be empty after destroy)
	@R="$(REGION)"; R=$${R:-$${AWS_REGION:-$(DEFAULT_REGION)}}; \
	echo "Checking for leftovers in $$R (the tagging index can lag a few minutes behind deletions)..."; \
	LEFT=$$(aws resourcegroupstaggingapi get-resources --region "$$R" \
	    --tag-filters Key=Project,Values=$(PROJECT) --query 'ResourceTagMappingList[].ResourceARN' --output text); \
	LOGS=$$(aws logs describe-log-groups --region "$$R" --log-group-name-prefix /aws/lambda/$(PROJECT) \
	    --query 'logGroups[].logGroupName' --output text); \
	if [ -z "$$LEFT$$LOGS" ]; then echo "OK: nothing left, nothing is being billed for this project."; \
	else echo "Still present (re-run in a few minutes, then delete by hand if still listed):"; \
	    echo "$$LEFT $$LOGS" | tr '\t ' '\n\n' | sed '/^$$/d;s/^/  - /'; exit 1; fi

.PHONY: clean
clean: ## Remove local build artifacts and the virtualenv (keeps Terraform state)
	rm -rf $(VENV) $(TF_DIR)/.build .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
