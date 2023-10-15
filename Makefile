TARGETS=test test_threadsafe test_pc bm hm_churn test_runner

FEATURE_FLAGS= 

ifdef QF_BITS_PER_SLOT
	FEATURE_FLAGS:=$(FEATURE_FLAGS) -D QF_BITS_PER_SLOT=$(QF_BITS_PER_SLOT)
else
	FEATURE_FLAGS:=$(FEATURE_FLAGS) -D QF_BITS_PER_SLOT=0
endif

ifdef BLOCKOFFSET
  ifeq ($(BLOCKOFFSET), NEW)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D_BLOCKOFFSET_4_NUM_RUNENDS
  else ifeq ($(BLOCKOFFSET), OLD)
  else
    $(error "Invalid BLOCKOFFSET=$(BLOCKOFFSET)")
  endif
else
  FEATURE_FLAGS:=$(FEATURE_FLAGS) -D_BLOCKOFFSET_4_NUM_RUNENDS
endif

ifdef UNORDERED
  FEATURE_FLAGS:=$(FEATURE_FLAGS) -D UNORDERED
endif

ifdef VAR
  ifeq ($(VAR), RHM)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_RHM
  else ifeq ($(VAR), TRHM)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_TRHM -D QF_TOMBSTONE
  else ifeq ($(VAR), GRHM)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_GRHM -D QF_TOMBSTONE -D AMORTIZED_REBUILD
  else ifeq ($(VAR), GRHM_NO_INSERT)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_GRHM_NO_INSERT -D QF_TOMBSTONE -D REBUILD_NO_INSERT -D AMORTIZED_REBUILD
  else ifeq ($(VAR), GZHM)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_GZHM -D QF_TOMBSTONE -D REBUILD_DEAMORTIZED_GRAVEYARD
  else ifeq ($(VAR), GZHM_UNORDERED)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_GZHM -D QF_TOMBSTONE -DUNORDERED -D REBUILD_DEAMORTIZED_GRAVEYARD
  else ifeq ($(VAR), GZHM_NO_INSERT)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_GZHM_NO_INSERT -D QF_TOMBSTONE -D REBUILD_NO_INSERT -D REBUILD_DEAMORTIZED_GRAVEYARD
  else ifeq ($(VAR), GZHM_INSERT)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_GZHM_INSERT -D QF_TOMBSTONE -D REBUILD_AT_INSERT
# Push each new ts to the next taken primitive ts postion, or the end of the cluster.
  else ifeq ($(VAR), GZHM_DELETE)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_GZHM_DELETE -D QF_TOMBSTONE -D DELETE_AND_PUSH
  else ifeq ($(VAR), CLHT)
    FEATURE_FLAGS:=$(FEATURE_FLAGS) -D USE_CLHT -DDEFAULT -Iexternal/clht/include -Iexternal/clht/external/include 
  else
    $(error "Invalid VAR=$(VAR)")
  endif
endif

ifdef PTS
	FEATURE_FLAGS:=$(FEATURE_FLAGS) -D PTS=$(PTS)
endif

ifdef D
	DEBUG=-g -DDEBUG=1
	OPT=
else
	DEBUG=
	OPT=-Ofast
endif

ifdef S
	STRICT= -DSTRICT=1
else
	STRICT=
endif

ifdef NH
	ARCH=
else
	ARCH=-msse4.2 -D__SSE4_2_
endif

ifdef P
	PROFILE=-pg -g -no-pie# for bug in gprof.
endif

LOC_INCLUDE=include
LOC_SRC=src
LOC_TEST=tests
LOC_BENCH=bench
OBJDIR=obj

CC = g++ -std=c++11
CXX = g++ -std=c++11
LD= g++ -std=c++11

# TODO: remove -Wno-unused-function and -Wno-unused-variable.
CXXFLAGS = -Wall $(DEBUG) $(PROFILE) $(OPT) $(ARCH) $(STRICT) $(FEATURE_FLAGS) -m64 -I. -Iinclude -Itests -Wno-unused-function -Wno-unused-variable
LDFLAGS = $(DEBUG) $(PROFILE) $(OPT) -lpthread -lssl -lcrypto -lm -lclht -Lexternal/clht

#
# declaration of dependencies
#

all: $(TARGETS)

# dependencies between programs and .o files

test:								$(OBJDIR)/test.o $(OBJDIR)/gqf.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

test_threadsafe:		$(OBJDIR)/test_threadsafe.o $(OBJDIR)/gqf.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

test_pc:						$(OBJDIR)/test_partitioned_counter.o $(OBJDIR)/gqf.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

bm:									$(OBJDIR)/bm.o $(OBJDIR)/gqf.o \
										$(OBJDIR)/zipf.o $(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

hm_churn:						$(OBJDIR)/hm_churn.o  $(OBJDIR)/gqf.o \
										$(OBJDIR)/hm.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

test_runner:				$(OBJDIR)/test_runner.o $(OBJDIR)/hm.o \
										$(OBJDIR)/gqf.o \
										$(OBJDIR)/hashutil.o \
										$(OBJDIR)/partitioned_counter.o

# dependencies between .o files and .h files

$(OBJDIR)/hm_churn.o:					$(LOC_TEST)/hm_wrapper.h

$(OBJDIR)/test_runner.o:			$(LOC_TEST)/hm_wrapper.h

$(OBJDIR)/hm_churn.o:					$(LOC_INCLUDE)/rhm_wrapper.h $(LOC_INCLUDE)/trhm_wrapper.h

$(OBJDIR)/test_runner.o:			$(LOC_INCLUDE)/rhm_wrapper.h $(LOC_INCLUDE)/trhm_wrapper.h


# dependencies between .o files and .cc (or .c) files

$(OBJDIR)/gqf.o:							$(LOC_SRC)/gqf.c \
															$(LOC_INCLUDE)/gqf.h \
															$(LOC_INCLUDE)/hashutil.h \
															$(LOC_INCLUDE)/util.h \
															$(LOC_INCLUDE)/ts_util.h

$(OBJDIR)/hashutil.o:					$(LOC_SRC)/hashutil.c $(LOC_INCLUDE)/hashutil.h
$(OBJDIR)/partitioned_counter.o:	$(LOC_INCLUDE)/partitioned_counter.h

#
# generic build rules
#

$(TARGETS):
	$(LD) $^ -o $@ $(LDFLAGS)

$(OBJDIR)/%.o: $(LOC_SRC)/%.cc | $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

$(OBJDIR)/%.o: $(LOC_SRC)/%.c | $(OBJDIR)
	$(CC) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

$(OBJDIR)/%.o: $(LOC_TEST)/%.cc | $(OBJDIR)
	$(CC) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

$(OBJDIR)/%.o: $(LOC_BENCH)/%.cc | $(OBJDIR)
	$(CC) $(CXXFLAGS) $(INCLUDE) $< -c -o $@

$(OBJDIR):
	@mkdir -p $(OBJDIR)

clean:
	rm -rf $(OBJDIR) $(TARGETS) core

