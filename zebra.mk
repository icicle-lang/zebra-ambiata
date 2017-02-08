# Which mafia to use. Caller can set a specific one as environment variable.
MAFIA ?= mafia

# Directory to store make dependencies and forget-me files
MAKE_DIR = dist/build/make
# Where cabal stores the build stuff that we have to clean
CABAL_OUT = dist/build

# Where to look for code.
# C code goes here. Sadly we can't just look in '.' because that would include lib submodules.
DIR_C = csrc
# Haskell code.
# One annoying thing is that the Haskell object files are in dist/build/Module.dyn_o
# rather than dist/build/src/Module.dyn_o,
# whereas C code are under dist/build/csrc/module.dyn_o.
DIR_HS = src

# Slurp all the .c and .hsc files
SRC_C = $(shell find $(DIR_C) -name "*.c")
SRC_HSC = $(shell find $(DIR_HS) -name "*.hsc")

# Convert each file to .dep, so that
# 	csrc/module.c => dist/build/make/csrc/module.dep
# and
# 	src/Module.hs => dist/build/make/Module.dep
DEP_C = $(patsubst %.c,$(MAKE_DIR)/%.dep,$(SRC_C))
DEP_HSC = $(patsubst $(DIR_HS)/%.hsc,$(MAKE_DIR)/%.dep,$(SRC_HSC))

# All the things we care about: dependency files and the forget-me files
TARGETS = \
	$(DEP_C) \
	$(DEP_HSC) \
	$(patsubst $(DIR_HS)/%.hsc,$(MAKE_DIR)/%.hs.forget,$(SRC_HSC)) \
	$(patsubst %.c,$(MAKE_DIR)/%.dyn_o.forget,$(SRC_C)) \

# Phony means these targets do not correspond to actual files
.PHONY: main clean

# Do nothing except make sure our dependencies are in order
main: $(TARGETS)
	@# And be quiet about it
	@:

# Blow it away...
clean:
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

# This is the same as the rule for C files, except we also need to give it the C source directory
# on the include path.
$(MAKE_DIR)/%.dep: $(MAKE_DIR)/%_hsc_make.c
	@echo "(HSC) Dependencies ------ $<"
	@mkdir -p $(@D)
	@gcc -MM -MT$(patsubst %.dep,%.hs.forget,$@) $< `$(MAFIA) cflags` -I$(DIR_C) > $@

# Now, we can update the forget-me file.
# This has no dependencies listed here: they are all specified in the generated dependency files.
%.forget :
	@echo "(Forget) ---------------- $(patsubst $(MAKE_DIR)/%.forget,$(CABAL_OUT)/%,$@)"
	@rm -f $(patsubst $(MAKE_DIR)/%.forget,$(CABAL_OUT)/%,$@)
	@mkdir -p $(@D)
	@touch $@

