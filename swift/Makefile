.DEFAULT_GOAL := build

.PHONY: clean build

clean:
	@rm -f .build/release
	@rm -rf .build/x86_64-apple-macosx
	@rm -rf .build/arm64-apple-macosx

build:
	@swift build -c release --arch x86_64
	@swift build -c release --arch arm64

