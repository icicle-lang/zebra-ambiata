# Zebra makefile
#
# Cabal is very good at compiling C source, but doesn't do a very good job of
# knowing *when* is should compile particular files.
# It doesn't perform any sort of dependency tracking, so if you modify a header
# then the source files won't be recompiled.
# We really want to fix this, but we still want to use Cabal because it has a
# few other nice things.
#
# The idea is to trick Cabal into recompiling source files by deleting the objects.
# We trick Make into doing this by having forget-me files for each source file:
# these are empty files which do nothing but have the right dependencies.
# Each forget-me file depends on the C file as well as the headers that the C file
# depends on.
# Then, when Make sees that one of these dependencies has changed, we remove the
# object file and touch the forget-me file to mark it as up to date.
#

# Which mafia to use. Caller can set a specific one as environment variable.
MAFIA ?= ./mafia

# Where to look for code.
# We should be able to reuse this for other projects just by modifying these two directories.
#
# C code goes here. We can't just look in '.' because that would include lib submodules.
# This should work with multiple entries (space separated).
DIR_C 	= csrc
# Haskell code.
# One annoying thing is that the Haskell object files are in dist/build/Module.dyn_o
# rather than dist/build/src/Module.dyn_o,
# whereas C code are under dist/build/csrc/module.dyn_o.
# This only works with a single entry.
DIR_HS 	= src


# Directory to store make dependencies and forget-me files
MAKE_DIR 	= dist/build/make
# Where cabal stores the build stuff that we have to clean
CABAL_OUT = dist/build

# Slurp all the .c and .hsc files
SRC_C 	= $(shell find $(DIR_C) -name "*.c")
SRC_HSC = $(shell find $(DIR_HS) -name "*.hsc")

# Convert each file to .dep, so that
# 	csrc/module.c => dist/build/make/csrc/module.dep
# and
# 	src/Module.hs => dist/build/make/Module.dep
DEP_C 	= $(patsubst %.c,$(MAKE_DIR)/%.dep,$(SRC_C))
DEP_HSC = $(patsubst $(DIR_HS)/%.hsc,$(MAKE_DIR)/%.dep,$(SRC_HSC))

# All the things we care about: dependency files and the forget-me files.
TARGETS 	= 																												\
	$(DEP_C) 																													\
	$(DEP_HSC) 																												\
	$(patsubst $(DIR_HS)/%.hsc,$(MAKE_DIR)/%.hs.forget,$(SRC_HSC)) 		\
	$(patsubst %.c,$(MAKE_DIR)/%.dyn_o.forget,$(SRC_C)) 							\


# Phony means these targets do not correspond to actual files
.PHONY: main clean-deps

# Do nothing except make sure our dependencies are in order
main: $(TARGETS)
	@# And be quiet about it
	@:

# Blow away the dependency and forget-me files.
# This doesn't clean the actual code.
clean-deps:
	@echo "Cleaning directory $(MAKE_DIR)"
	@rm -r $(MAKE_DIR)

# Now include all the generated dependency files.
# This has to go after main because the first target is the default.
-include $(DEP_HSC)
-include $(DEP_C)

# If you want to build a dependency file from a C file, you can:
# The dependency should have the forget-me file as the target, so
# the file csrc/module.dep contains:
#
# > dist/build/make/csrc/module.dyn_o.forget: csrc/module.c csrc/module.h
#
$(MAKE_DIR)/%.dep : %.c
	@echo "(C) Dependencies -------- $<"
	@# Ensure the directory exists before we try to put things in it.
	@mkdir -p $(@D)
	@gcc -MM -MT $(patsubst %.c,$(MAKE_DIR)/%.dyn_o.forget,$<) $< `$(MAFIA) cflags` > $@

# Convert the .hsc into C code so we can compute its dependencies.
$(MAKE_DIR)/%_hsc_make.c: $(DIR_HS)/%.hsc
	@echo "(HSC) Converting -------- $<"
	@mkdir -p $(@D)
	@hsc2hs $< -o $(patsubst %_hsc_make.c,%,$@) --no-compile

DIR_C_INCLUDES = $(patsubst %,-I%,$(DIR_C))

# This is the same as the rule for C files, except we also need to give it the C source directory
# on the include path.
$(MAKE_DIR)/%.dep: $(MAKE_DIR)/%_hsc_make.c
	@echo "(HSC) Dependencies ------ $<"
	@mkdir -p $(@D)
	@gcc -MM -MT$(patsubst %.dep,%.hs.forget,$@) $< `$(MAFIA) cflags` $(DIR_C_INCLUDES) > $@

# Now, we can update the forget-me file.
# This has no dependencies listed here: they are all specified in the generated dependency files.
%.forget :
	@echo "(Forget) ---------------- $(patsubst $(MAKE_DIR)/%.forget,$(CABAL_OUT)/%,$@)"
	@rm -f $(patsubst $(MAKE_DIR)/%.forget,$(CABAL_OUT)/%,$@)
	@mkdir -p $(@D)
	@touch $@

