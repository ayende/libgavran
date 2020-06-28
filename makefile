# the compiler to use
CC = clang

# compiler flags:
#  -g    adds debugging information to the executable file
#  -Wall turns on most, but not all, compiler warnings
  
TARGET_EXEC ?= gavran

BUILD_DIR ?= ./build
SRC_DIRS ?= ./

SRCS := $(shell find $(SRC_DIRS) -name '*.c')
OBJS := $(SRCS:%=$(BUILD_DIR)/%.o)
DEPS := $(OBJS:.o=.d)

INC_DIRS := $(she ll find $(SRC_DIRS) -type d)
INC_FLAGS := $(addprefix -I,$(INC_DIRS))

WARNINGS = -Weverything -Werror -Wno-gnu-zero-variadic-macro-arguments -Wno-pointer-arith -Wno-reserved-id-macro
DEFINES = -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE

CFLAGS  = -g $(WARNINGS) $(INC_FLAGS) -MMD -MP $(DEFINES) -fPIC  $(ASAN)

LDFLAGS = -lm -lsodium -lzstd #-shared

$(BUILD_DIR)/$(TARGET_EXEC): $(OBJS)
	$(CC) $(OBJS) -o $@.so $(LDFLAGS) -shared
	$(CC) $(OBJS) -o $@ $(LDFLAGS) 

# c source 
$(BUILD_DIR)/%.c.o: %.c
	$(MKDIR_P) $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

.PHONY: clean

clean:
	$(RM) -r $(BUILD_DIR)

-include $(DEPS)

MKDIR_P ?= mkdir -p


