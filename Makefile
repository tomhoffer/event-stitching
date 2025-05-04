.PHONY: test profile view-cpu view-mem view-block

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run benchmarks with profiling
profile:
	@echo "Running benchmarks with profiling..."
	@go test -bench=. -cpuprofile=cpu.prof -memprofile=mem.prof -blockprofile=block.prof -benchtime=20s ./internal/tools/benchmarks

# View CPU profile
view-cpu:
	@go tool pprof cpu.prof

# View memory profile
view-mem:
	@go tool pprof mem.prof

# View block profile
view-block:
	@go tool pprof block.prof

# Help
help:
	@echo "Available targets:"
	@echo "  test       - Run tests"
	@echo "  profile    - Run benchmarks with profiling"
	@echo "  view-cpu   - View CPU profile"
	@echo "  view-mem   - View memory profile"
	@echo "  view-block - View block profile"
	@echo "  help       - Show this help message" 