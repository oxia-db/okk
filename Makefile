
.PHONY: build-manager
build-manager:
	$(MAKE) -C manager docker-build IMG=mattison/okk-manager:0.1.0