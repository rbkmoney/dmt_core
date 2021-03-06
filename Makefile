REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
SUBMODULES = builtils
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

UTILS_PATH := builtils
TEMPLATES_PATH := .

# Name of the service
SERVICE_NAME := dmt_core

# Build image tag to be used
BUILD_IMAGE_TAG := 0c638a682f4735a65ef232b81ed872ba494574c3

CALL_ANYWHERE := all submodules rebar-update compile xref lint dialyze test clean distclean check_format format
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
	$(REBAR) as test xref

lint:
	elvis rock

check_format:
	$(REBAR) as test fmt -c

format:
	$(REBAR) fmt -w

dialyze:
	$(REBAR) as test dialyzer

test:
	$(REBAR) eunit
	$(REBAR) ct

clean:
	$(REBAR) clean

distclean:
	rm -rf _build
