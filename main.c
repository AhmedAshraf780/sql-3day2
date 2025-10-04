#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#define COLUMN_SIZE_NAME 32
#define COLUMN_SIZE_EMAIL 255
#define TABLE_MAX_PAGES 100
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

typedef struct {
  uint32_t id;
  char username[COLUMN_SIZE_NAME];
  char email[COLUMN_SIZE_EMAIL];
} Row;

// size of attrs
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);

// offsets : we use offset to know where should i place this data in the memory
// for example I only know the address 0X00 , based on the username offset value
// i can set the username value at 0X00 + username offset , and this gurantee
// that you have contigous data in the memory
// -------------------------------------------------
//  ID     |    username     | email
// -------------------------------------------------
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/*
 * ---------------------------------------
 *          TABLE Attribute
 *  ------------------------------------
 *
 * */
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
  uint32_t num_rows;
  void *pages[TABLE_MAX_PAGES];
} Table;

/*
 *    TYPEDEV
 *
 * */

typedef struct {
  char *buffer; // this will contain all the what's gonna writen including the
                // trailing line
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

// enum for meta-keyword
typedef enum { META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED } META_RESULT;

// enum for preparation
typedef enum {
  PREPARE_SUCESS,
  PREPARE_UNRECOGNIZED,
  PREPARE_SYNTAX_ERROR
} PREPARE_RESULT;

// enum for statments
typedef enum { INSERT_STATMENT, SELECT_STATMENT } STATMENT_TYPE;

// struct for statments type
typedef struct {
  STATMENT_TYPE type;
  Row row_to_insert;
} Statment;

/*
 *    TYPEDEV
 *
 * */

InputBuffer *new_input_buffer() {
  InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void print_prompt() { printf("sqlite > "); }

void read_input(InputBuffer *input_buffer) {
  // validate the what's written len
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error Reading input\n");
    exit(EXIT_FAILURE);
  }

  // Ignoring trailing ???
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer *input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

META_RESULT do_meta_command(InputBuffer *input_buffer) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED;
  }
}

PREPARE_RESULT prepare_statment(InputBuffer *input_buffer, Statment *statment) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statment->type = INSERT_STATMENT;
    // validation for number of args
    int args = sscanf(
        input_buffer->buffer, "insert %d %31s %254s", &(statment->row_to_insert.id),
        statment->row_to_insert.username, statment->row_to_insert.email);

    if (args < 3) {
      return PREPARE_SYNTAX_ERROR;
    }
    printf("DEBUG: id=%d, user=%s, email=%s\n", statment->row_to_insert.id,
           statment->row_to_insert.username, statment->row_to_insert.email);
    return PREPARE_SUCESS;
  } else if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    statment->type = SELECT_STATMENT;
    return PREPARE_SUCESS;
  }

  return PREPARE_UNRECOGNIZED;
}

/* ---------------------------------------------------------------
 *                 SERIALIZATION
 * ----------------------------------------------------
 * */
void serialize_row(Row *source, void *destination) {
  // destination is the address we will start storing from
  memcpy((char *)destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy((char *)destination + USERNAME_OFFSET, source->username,
         USERNAME_SIZE);
  memcpy((char *)destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
  // deserialize means that we are gonna fetch these data that stored with
  // serialize_row function
  memcpy(&(destination->id), (char *)source + ID_OFFSET, ID_SIZE);
  memcpy(destination->username, (char *)source + USERNAME_OFFSET,
         USERNAME_SIZE);
  memcpy(destination->email, (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *row_slot(Table *table, uint32_t row_num) {
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void *page = table->pages[page_num];
  if (page == NULL) {
    page = table->pages[page_num] = malloc(PAGE_SIZE);
  }

  // then we need to know where should i put this rowa think of it as books if
  // the shelf is maximux can contain 2 books row1 row2 so row3 will be in the
  // other page row1   row3(here we got this place from the equation (row_num %
  // ROWS_PER_PAGE)) row2
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t bytes_offset = row_offset * ROW_SIZE;
  return page + bytes_offset;
}

/**
 *   EXECUTION PROCESS
 */
typedef enum { EXECUTE_TABLE_FULL, EXECUTE_SUCCESS } ExecuteResult;
ExecuteResult execute_insert(Statment *statment, Table *table) {
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }
  // row to insert
  Row *row_to_insert = &statment->row_to_insert;
  serialize_row(row_to_insert, row_slot(table, table->num_rows));
  table->num_rows += 1;

  return EXECUTE_SUCCESS;
}

void print_row(Row *row) {
  printf("---------------------------------\n");
  printf("| %d\n", row->id);
  printf("| %s\n", row->username);
  printf("| %s\n", row->email);
  printf("---------------------------------\n");
}

ExecuteResult execute_select(Statment *statment, Table *table) {
  Row row;
  for (uint32_t i = 0; i < table->num_rows; i++) {
    deserialize_row(row_slot(table, i), &row);
    print_row(&row);
  }
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statment(Statment *statment, Table *table) {
  switch (statment->type) {
  case INSERT_STATMENT:
    return execute_insert(statment, table);
  case SELECT_STATMENT:
    return execute_select(statment, table);
  }
}

Table *new_table() {
  Table *table = (Table *)malloc(sizeof(Table));
  table->num_rows = 0;
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    table->pages[i] = NULL;
  }
  return table;
}
void free_table(Table *table) {
  for (int i = 0; table->pages[i]; i++)
    free(table->pages[i]);

  free(table);
}

int main(int argc, char *argv[]) {

  /*
   * Create Table
   * */
  Table *table = new_table();

  InputBuffer *input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer)) {
      case META_COMMAND_SUCCESS:
        continue;
      case META_COMMAND_UNRECOGNIZED:
        printf("Unrecognized meta command\n");
        continue;
      };
    }

    Statment statment;
    switch (prepare_statment(input_buffer, &statment)) {
    case PREPARE_SUCESS:
      break;
    case PREPARE_SYNTAX_ERROR:
      printf("Couldn't parse statment, syntax error");
      continue;
    case PREPARE_UNRECOGNIZED:
      printf("Unrecognized Statment\n");
      continue;
    }

    switch (execute_statment(&statment, table)) {
    case EXECUTE_SUCCESS:
      printf("Executed \n");
      break;
    case EXECUTE_TABLE_FULL:
      printf("Error Table is full");
      break;
    }
  }
  return 0;
}
