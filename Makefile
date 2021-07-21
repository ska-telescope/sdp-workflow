.PHONY: help release-patch release-minor release-major

help: ## Show this help
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' ./Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2}'

release-patch: ## Make patch release
	bumpver update --patch -n

release-minor: ## Make minor release
	bumpver update --minor -n

release-major: ## Make major release
	bumpver update --major -n
