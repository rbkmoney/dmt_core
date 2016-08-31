REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)
SUBMODULES = damsel
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

.PHONY:  submodules rebar-update compile xref lint dialyze start devrel release clean distclean all

all: compile

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

clean:
	$(REBAR) clean

distclean:
	$(REBAR) clean -a
	rm -rfv _build _builds _cache _steps _temp
