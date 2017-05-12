include ../vsn.mk

# ----------------------------------------------------
# VARIABLES
# ----------------------------------------------------
APP_NAME = circdb

MODULES = \
	circdb \
	circdb_app \
	circdb_sup \
	circdb_backup \
	circdb_manager \
	circdb_table \
	circdb_lib

TARGET_FILES = $(EMAKE_TARGET) $(DOC_TARGET_FILES) $(APP_TARGET)


# Erlang include directories. 
INCLUDES = \
	../include

include ../../meadow/priv/Makefile.vars

# ----------------------------------------------------
# BUILD TARGETS
# ----------------------------------------------------

all: $(TARGET_FILES) 
	erl -make

clean:
	rm -f $(APP_TARGET) $(EMAKE_TARGET) ../ebin/*.beam

doc: edoc

include ../../meadow/priv/Makefile.rules

# ----------------------------------------------------
# Special Build Targets
# ----------------------------------------------------

