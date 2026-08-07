# Image URL to use all building/pushing image targets
IMG ?= controller:latest

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

CONTAINER_TOOL ?= docker
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: build
build: ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: ## Run controller from your host.
	go run ./cmd/main.go

.PHONY: fmt
fmt: ## Run go fmt.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet.
	go vet ./...

.PHONY: lint
lint: golangci-lint ## Run golangci-lint.
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint with auto-fix.
	$(GOLANGCI_LINT) run --fix

.PHONY: govulncheck
govulncheck: govulncheck-tool ## Scan dependencies for known vulnerabilities.
	$(GOVULNCHECK) ./...

##@ Code Generation

.PHONY: generate
generate: controller-gen ## Generate DeepCopy methods.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: manifests
manifests: controller-gen ## Generate CRD and RBAC manifests.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: sync-crds
sync-crds: manifests ## Sync generated CRDs into Helm chart templates.
	@echo "Syncing CRDs into charts/drop-crds/templates/ and charts/drop/templates/"
	@mkdir -p charts/drop-crds/templates charts/drop/templates
	@for f in config/crd/bases/*.yaml; do \
		base=$$(basename "$$f"); \
		{ echo '{{- /* Generated from config/crd/bases — do not edit manually. Run make sync-crds */ -}}'; \
		  echo '{{- if .Values.install }}'; cat "$$f"; echo '{{- end }}'; \
		} > "charts/drop-crds/templates/$$base"; \
		{ echo '{{- /* Generated from config/crd/bases — do not edit manually. Run make sync-crds */ -}}'; \
		  echo '{{- if .Values.crds.install }}'; cat "$$f"; echo '{{- end }}'; \
		} > "charts/drop/templates/crds-$$base"; \
	done

.PHONY: codegen
codegen: generate manifests sync-crds docs-gen ## Run all code generation (deepcopy + CRDs + docs).

##@ Testing

.PHONY: test
test: setup-envtest ## Run unit tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

.PHONY: test-e2e
test-e2e: chainsaw ## Run Chainsaw E2E tests (requires kind cluster).
	$(CHAINSAW) test test/e2e/

##@ Cluster

.PHONY: kind-create
kind-create: ## Create kind cluster for development.
	$(KIND) create cluster --name drop-dev --config hack/kind-config.yaml --wait 5m

.PHONY: kind-delete
kind-delete: ## Delete the kind cluster.
	$(KIND) delete cluster --name drop-dev

.PHONY: install
install: manifests kustomize ## Install CRDs into cluster.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from cluster.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found -f -

.PHONY: e2e-infra
e2e-infra: ## Deploy Prometheus, Loki, and Registry for E2E/dev.
	@chmod +x hack/e2e-infra/setup.sh && hack/e2e-infra/setup.sh

##@ Docker

.PHONY: docker-build
docker-build: ## Build docker image.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image.
	$(CONTAINER_TOOL) push ${IMG}

.PHONY: kind-load
kind-load: docker-build ## Build and load image into kind.
	$(KIND) load docker-image ${IMG} --name drop-dev

##@ Helm & Docs

DOCS_IMG_SRC_DIR ?= docs/static/images/src
DOCS_IMG_VENV ?= $(DOCS_IMG_SRC_DIR)/.venv

.PHONY: helm-lint
helm-lint: ## Lint the Helm chart.
	helm lint charts/drop

.PHONY: helm-template
helm-template: ## Render Helm templates locally.
	helm template drop charts/drop

.PHONY: docs-serve
docs-serve: ## Serve Hugo docs locally.
	cd docs && hugo server --buildDrafts --port 1313

.PHONY: docs-gen
docs-gen: ## Regenerate AI agent docs (llms.txt, instructions, etc.) from source.
	go run ./hack/gen-ai-docs/

.PHONY: docs-gen-check
docs-gen-check: docs-gen ## Verify generated AI docs are up to date.
	@git diff --exit-code knowledge.yaml llms.txt llms-full.txt docs/static/llms-full.txt .github/copilot-instructions.md .cursorrules AGENTS.md docs/content/docs/reference/_generated_*.md || \
		(echo "ERROR: generated docs are out of date — run 'make docs-gen'" && exit 1)

.PHONY: docs-images-setup
docs-images-setup: ## Create docs image venv and install chart dependencies.
	@cd $(DOCS_IMG_SRC_DIR) && \
	python3 -m venv .venv && \
	. .venv/bin/activate && \
	pip install -r requirements.txt

.PHONY: docs-images-gen
docs-images-gen: docs-images-setup ## Generate docs SVG charts from sample data.
	@cd $(DOCS_IMG_SRC_DIR) && \
	. .venv/bin/activate && \
	python signal-eventpulltime.py && \
	python discovery-charts.py && \
	python research-doc-assets.py && \
	python research-tikz-assets.py

.PHONY: static
static: docs-images-gen ## Generate all maintained static docs graphics.

##@ Research

RESEARCH_TEX_DIR ?= research/tex
RESEARCH_TEX_FILE ?= paper.tex
RESEARCH_BENCH_DIR ?= research/benchmark/evaluator
RESEARCH_BENCH_VENV ?= $(RESEARCH_BENCH_DIR)/.venv
RESEARCH_BENCH_RESULTS_DIR ?= research/benchmark/results
RESEARCH_BENCH_RESULTS_DISCOVERY_20RUNS ?= $(RESEARCH_BENCH_RESULTS_DIR)/discovery-strategy-20runs
RESEARCH_BENCH_RESULTS_ORACLE_20RUNS ?= $(RESEARCH_BENCH_RESULTS_DIR)/oracle-gap-strategy-20runs
RESEARCH_BENCH_RESULTS_CACHE_20RUNS ?= $(RESEARCH_BENCH_RESULTS_DIR)/ci-image-cache-20runs

.PHONY: research-tex-build
research-tex-build: ## Build research PDF from TeX source (override RESEARCH_TEX_FILE=<file.tex>).
	@cd $(RESEARCH_TEX_DIR) && \
	if command -v latexmk >/dev/null 2>&1; then \
		latexmk -pdf -interaction=nonstopmode -halt-on-error $(RESEARCH_TEX_FILE); \
	elif command -v pdflatex >/dev/null 2>&1; then \
		pdflatex -interaction=nonstopmode -halt-on-error $(RESEARCH_TEX_FILE) && \
		pdflatex -interaction=nonstopmode -halt-on-error $(RESEARCH_TEX_FILE); \
	else \
		echo "ERROR: latexmk/pdflatex not found"; exit 1; \
	fi

.PHONY: research-bench-setup
research-bench-setup: ## Create benchmark venv and install Python dependencies.
	@cd $(RESEARCH_BENCH_DIR) && \
	python3 -m venv .venv && \
	. .venv/bin/activate && \
	pip install -r requirements.txt

.PHONY: research-bench-generate
research-bench-generate: ## Generate synthetic benchmark dataset.
	@cd $(RESEARCH_BENCH_DIR) && \
	. .venv/bin/activate && \
	python generate_synthetic_day.py --out data --jobs 25000 --nodes 100 --images 30 --seed 20260621

.PHONY: research-bench-replay
research-bench-replay: ## Run replay policy evaluation from benchmark data.
	@cd $(RESEARCH_BENCH_DIR) && \
	. .venv/bin/activate && \
	python evaluate_replay.py --data data --out outputs

.PHONY: research-bench-discovery
research-bench-discovery: ## Evaluate discovery strategies from benchmark data.
	@cd $(RESEARCH_BENCH_DIR) && \
	. .venv/bin/activate && \
	python evaluate_discovery_strategies.py --data data --out outputs/strategy_eval

.PHONY: research-bench-plot
research-bench-plot: ## Render example pipeline Gantt figure.
	@cd $(RESEARCH_BENCH_DIR) && \
	. .venv/bin/activate && \
	python plot_pipeline_gantt.py --modeled-jobs outputs/modeled_jobs_no_prewarming.csv --out figures/example_gantt.png

.PHONY: research-bench-20runs
research-bench-20runs: ## Run 20-run discovery strategy benchmark batch.
	@cd $(RESEARCH_BENCH_DIR) && \
	. .venv/bin/activate && \
	python run_discovery_strategy_20runs.py

.PHONY: research-bench-all
research-bench-all: research-bench-generate research-bench-replay research-bench-discovery research-bench-plot ## Run full synthetic benchmark workflow.

.PHONY: tools
tools: ## Install local tooling and check optional docs/chart binaries.
	@$(MAKE) kustomize controller-gen setup-envtest golangci-lint chainsaw
	@command -v hugo >/dev/null 2>&1 || echo "WARNING: hugo not found — needed for docs"
	@command -v helm >/dev/null 2>&1 || echo "WARNING: helm not found — needed for chart dev"

##@ Tool Dependencies

LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
CHAINSAW ?= $(LOCALBIN)/chainsaw
GOVULNCHECK ?= $(LOCALBIN)/govulncheck

KUSTOMIZE_VERSION ?= v5.6.0
CONTROLLER_TOOLS_VERSION ?= v0.17.2
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v2.12.2
CHAINSAW_VERSION ?= v0.2.15
GOVULNCHECK_VERSION ?= v1.6.0

.PHONY: kustomize
kustomize: $(KUSTOMIZE)
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN)
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: $(ENVTEST)
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT)
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

.PHONY: chainsaw
chainsaw: $(CHAINSAW)
$(CHAINSAW): $(LOCALBIN)
	$(call go-install-tool,$(CHAINSAW),github.com/kyverno/chainsaw,$(CHAINSAW_VERSION))

.PHONY: govulncheck-tool
govulncheck-tool: $(GOVULNCHECK)
$(GOVULNCHECK): $(LOCALBIN)
	$(call go-install-tool,$(GOVULNCHECK),golang.org/x/vuln/cmd/govulncheck,$(GOVULNCHECK_VERSION))

define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) GOTOOLCHAIN=local go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef
