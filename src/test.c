#include "rdb_parser.h"

int main(int argc, char* argv[]) {
	int i;

    rdb_parser_t *rp = create_rdb_parser(4096);

	for (i = 0; i < 1; ++i) {
		//rdb_parse_file(rp, "data/dump2.8.rdb");
		rdb_parse_file(rp, "data/dump3.rdb");
		rdb_dump(rp, "data/dump3.txt");
		reset_rdb_parser(rp);
	}

    destroy_rdb_parser(rp);
    return 0;
}