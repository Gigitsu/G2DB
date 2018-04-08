#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <memory.h>
#include <fcntl.h>
#include <unistd.h>

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0) -> Attribute)

const uint32_t COLUMNS_USERNAME_SIZE = 32;
const uint32_t COLUMNS_EMAIL_SIZE = 255;

enum MetaCommandResult_t {
    META_COMMAND_SUCCESS, META_COMMAND_FAILURE
};
typedef enum MetaCommandResult_t MetaCommandResult;

enum PrepareStatementResult_t {
    PREPARE_STATEMENT_SUCCESS,
    PREPARE_STATEMENT_FAILURE,
    PREPARE_STATEMENT_NEGATIVE_ID,
    PREPARE_STATEMENT_SYNTAX_ERROR,
    PREPARE_STATEMENT_STRING_TOO_LONG
};
typedef enum PrepareStatementResult_t PrepareStatementResult;

enum StatementType_t {
    STATEMENT_INSERT, STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

enum ExecuteResult_t {
    EXECUTE_SUCCESS, EXECUTE_TABLE_FULL
};
typedef enum ExecuteResult_t ExecuteResult;

struct Row_t {
    uint32_t id;
    char email[COLUMNS_EMAIL_SIZE + 1];
    char username[COLUMNS_USERNAME_SIZE + 1];
};
typedef struct Row_t Row;

struct Statement_t {
    StatementType type;
    Row row_to_insert;
};
typedef struct Statement_t Statement;

struct InputBuffer_t {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = EMAIL_OFFSET + EMAIL_SIZE;

void serialize_row(Row *src, void *dst) {
    memcpy(dst + ID_OFFSET, &(src->id), ID_SIZE);
    memcpy(dst + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
    memcpy(dst + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
}

void deserialize_row(void *src, Row *dst) {
    memcpy(&(dst->id), src + ID_OFFSET, ID_SIZE);
    memcpy(&(dst->email), src + EMAIL_OFFSET, EMAIL_SIZE);
    memcpy(&(dst->username), src + USERNAME_OFFSET, USERNAME_SIZE);
}

const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Pager_t {
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

struct Table_t {
    Pager *pager;
    uint32_t num_rows;
};
typedef struct Table_t Table;

Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    Table *tbl = malloc(sizeof(Table));
    tbl->num_rows = num_rows;
    tbl->pager = pager;
    return tbl;
}

void db_close(Table *tbl) {
    Pager *pager = tbl->pager;
    uint32_t num_full_pages = tbl->num_rows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_full_pages; ++i) {
        if (pager->pages[i] == NULL) continue;

        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    uint32_t num_additional_rows = tbl->num_rows % ROWS_PER_PAGE;

    if (num_additional_rows > 0) {
        uint32_t page_num = num_full_pages;
        if (pager->pages[page_num] != NULL) {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int result = close(pager->file_descriptor);

    if (result == -1) {
        printf("Error closing db file\n");
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        void *page = pager->pages[i];
        if(page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
}

Pager *pager_open(const char *filename) {
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager[i] = NULL;
    }

    return pager;
}

void *get_page(Pager *pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;
    }

    return pager->pages[page_num];
}

void *row_slot(Table *tbl, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = get_page(tbl->pager, page_num);

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;

    return page + byte_offset;
}

InputBuffer *new_input_buffer() {
    InputBuffer *ib = malloc(sizeof(InputBuffer));
    ib->buffer = NULL;
    ib->buffer_length = 0;
    ib->input_length = 0;

    return ib;
}

void print_row(Row *row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void print_prompt() { printf("g2db> "); }

void read_input(InputBuffer *ib) {
    ssize_t bytes_read = getline(&(ib->buffer), &(ib->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // ignore trailing newline
    ib->input_length = bytes_read - 1;
    ib->buffer[bytes_read - 1] = 0;
}

MetaCommandResult do_meta_command(InputBuffer *ib, Table *tbl) {
    if (strcmp(ib->buffer, ".exit") == 0) {
        db_close(tbl);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_FAILURE;
    }
}

PrepareStatementResult prepare_insert(InputBuffer *ib, Statement *stm) {
    stm->type = STATEMENT_INSERT;

    char *keyword = strtok(ib->buffer, " ");
    char *id_str = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if (id_str == NULL || username == NULL || email == NULL) {
        return PREPARE_STATEMENT_SYNTAX_ERROR;
    }

    int id = atoi(id_str);

    if (id < 0) {
        return PREPARE_STATEMENT_NEGATIVE_ID;
    }

    if (strlen(username) > COLUMNS_USERNAME_SIZE || strlen(email) > COLUMNS_EMAIL_SIZE) {
        return PREPARE_STATEMENT_STRING_TOO_LONG;
    }

    stm->row_to_insert.id = id;
    strcpy(stm->row_to_insert.email, email);
    strcpy(stm->row_to_insert.username, username);

    return PREPARE_STATEMENT_SUCCESS;
}

PrepareStatementResult prepare_statement(InputBuffer *ib, Statement *stm) {
    if (strncmp(ib->buffer, "insert", 6) == 0) {
        return prepare_insert(ib, stm);
    }

    if (strcmp(ib->buffer, "select") == 0) {
        stm->type = STATEMENT_SELECT;
        return PREPARE_STATEMENT_SUCCESS;
    }

    return PREPARE_STATEMENT_FAILURE;
}

ExecuteResult execute_insert(Statement *stm, Table *tbl) {
    if (tbl->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &(stm->row_to_insert);

    serialize_row(row_to_insert, row_slot(tbl, tbl->num_rows));

    tbl->num_rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *stm, Table *tbl) {
    Row row;

    for (int i = 0; i < tbl->num_rows; i++) {
        deserialize_row(row_slot(tbl, i), &row);
        print_row(&row);
    }

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *stm, Table *tbl) {
    switch (stm->type) {
        case STATEMENT_SELECT:
            return execute_select(stm, tbl);
        case STATEMENT_INSERT:
            return execute_insert(stm, tbl);
    }
}

int main(int argc, char *argv[]) {
    Table *tbl = new_table();

    InputBuffer *ib = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(ib);

        if (ib->buffer[0] == '.') {
            switch (do_meta_command(ib, tbl)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_FAILURE):
                    printf("Unrecognized command '%s'\n", ib->buffer);
            }
        }

        Statement stm;
        switch (prepare_statement(ib, &stm)) {
            case (PREPARE_STATEMENT_SUCCESS):
                break;
            case (PREPARE_STATEMENT_SYNTAX_ERROR):
                printf("Syntax error.\n");
                continue;
            case (PREPARE_STATEMENT_NEGATIVE_ID):
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_STATEMENT_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_STATEMENT_FAILURE):
                printf("Unrecognized comand at start of '%s'\n", ib->buffer);
                continue;
        }

        switch (execute_statement(&stm, tbl)) {
            case EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;
            case EXECUTE_TABLE_FULL:
                printf("Error: Table full.\n");
                break;
        }
    }
}