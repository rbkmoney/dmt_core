REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
SUBMODULES = builtils
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

UTILS_PATH := builtils
TEMPLATES_PATH := .

# Name of the service
SERVICE_NAME := dmt_core

# Build image tag to be used
BUILD_IMAGE_TAG := b40627de232e7f04c9abd2c480856dac7bcd9386

CALL_ANYWHERE := all submodules rebar-update compile xref lint dialyze test clean distclean
CALL_W_CONTAINER := $(CALL_ANYWHERE)

all: compile

-include $(UTILS_PATH)/make_lib/utils_container.mk

.PHONY: $(CALL_W_CONTAINER)

$(SUBTARGETS): %/.git: %
	git submodule update --init $<
	touch $@

submodules: $(SUBTARGETS)

rebar-update:
	$(REBAR) update

compile: submodules rebar-update
	$(REBAR) compile

xref: submodules
	$(REBAR) xref

lint:
	elvis rock

dialyze:
	$(REBAR) dialyzer

test:
	$(REBAR) eunit
	$(REBAR) ct

clean:
	$(REBAR) clean

distclean:
	rm -rf _build
