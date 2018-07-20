#include "rdb_parser/rdb_parser.h"

#include <time.h>

struct file_builder {
	const char *path;
	const char *dump_to_path;
	FILE *fp;

	int total;
};

static int
__dump_node(rdb_node_t *node, struct file_builder *fb)
{
	rdb_kv_t *kv;
	rdb_kv_chain_t *kvcl;

	FILE *fp = fb->fp;
	
	++fb->total;

	switch (node->type) {
	case REDIS_AUX_FIELDS:
		fprintf(fp, "(%d)[AUX_FIELDS] %s\n\n",
			fb->total, node->aux_fields.data);
		break;

	case REDIS_EXPIRETIME:
	case REDIS_EXPIRETIME_MS:
		fprintf(fp, "(%d)[EXPIRE] %d\n\n",
			fb->total, node->expire);
		break;

	case REDIS_SELECTDB:
		fprintf(fp, "(%d)[DB_SELECTOR] %d\n\n",
			fb->total, node->db_selector);
		break;

	case REDIS_STRING:
		fprintf(fp, "(%d)[STRING] %s = %s\n\n",
			fb->total, node->key.data, node->val.data);
		break;

	case REDIS_LIST:
		fprintf(fp, "(%d)[LIST] %s = (%d)\n[\n",
			fb->total, node->key.data, node->size);

		for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
			kv = kvcl->kv;
			fprintf(fp, "\t%s,\n", kv->val.data);
		}
		fprintf(fp, "]\n\n");
		break;

	case REDIS_SET:
		fprintf(fp, "(%d)[SET] %s = (%d)\n[\n",
			fb->total, node->key.data, node->size);

		for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
			kv = kvcl->kv;
			fprintf(fp, "\t%s,\n", kv->val.data);
		}
		fprintf(fp, "]\n\n");
		break;

	case REDIS_ZSET:
		fprintf(fp, "(%d)[ZSET] %s = (%d)\n[\n",
			fb->total, node->key.data, node->size);

		for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
			kv = kvcl->kv;
			fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
		}
		fprintf(fp, "]\n\n");
		break;

	case REDIS_HASH:
		fprintf(fp, "(%d)[HASH] %s = (%d)\n[\n",
			fb->total, node->key.data, node->size);

		for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
			kv = kvcl->kv;
			fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
		}
		fprintf(fp, "]\n\n");
		break;

	case REDIS_HASH_ZIPMAP:
		fprintf(fp, "(%d)[ZIPMAP] %s = (%d)\n[\n",
			fb->total, node->key.data, node->size);

		for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
			kv = kvcl->kv;

			if (kv->key.len > 0) {
				fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
			}
			else {
				fprintf(fp, "\t%s,\n", kv->val.data);
			}
		}
		fprintf(fp, "]\n\n");
		break;

	case REDIS_LIST_ZIPLIST:
		fprintf(fp, "(%d)[ZIPLIST] %s = (%d)\n[\n",
			fb->total, node->key.data, node->size);

		for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
			kv = kvcl->kv;

			if (kv->key.len > 0) {
				fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
			}
			else {
				fprintf(fp, "\t%s,\n", kv->val.data);
			}
		}
		fprintf(fp, "]\n\n");
		break;

	case REDIS_SET_INTSET:
		fprintf(fp, "(%d)[INTSET] %s = (%d)\n[\n",
			fb->total, node->key.data, node->size);

		for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
			kv = kvcl->kv;

			if (kv->key.len > 0) {
				fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
			}
			else {
				fprintf(fp, "\t%s,\n", kv->val.data);
			}
		}
		fprintf(fp, "]\n\n");
		break;

	case REDIS_ZSET_ZIPLIST:
		fprintf(fp, "(%d)[ZSET_ZL] %s = (%d)\n[\n",
			fb->total, node->key.data, node->size);

		for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
			kv = kvcl->kv;

			if (kv->key.len > 0) {
				fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
			}
			else {
				fprintf(fp, "\t%s,\n", kv->val.data);
			}
		}
		fprintf(fp, "]\n\n");
		break;

	case REDIS_HASH_ZIPLIST:
		fprintf(fp, "(%d)[HASH_ZL] %s = (%d)\n[\n",
			fb->total, node->key.data, node->size);

		for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
			kv = kvcl->kv;

			if (kv->key.len > 0) {
				fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
			}
			else {
				fprintf(fp, "\t%s,\n", kv->val.data);
			}
		}
		fprintf(fp, "]\n\n");
		break;

	case REDIS_EOF:
	default:
		break;
	}

	return 0;
}

static int
on_build_node(rdb_node_t *n, void *payload) {
	struct file_builder *fb = (struct file_builder *)payload;
	__dump_node(n, fb);
	return 0;
}

int main(int argc, char* argv[]) {
	int i, count;
	time_t tmstart, tmover;
	time_t tmstart2, tmover2;
	struct file_builder fb;

	rdb_parser_t *rp;

	count = 10;
	tmstart = time(NULL);

	rp = create_rdb_parser(on_build_node, &fb);

	for (i = 0; i < count; ++i) {

		//fb.path = "data/dump2.8.rdb";
		fb.path = "data/dump3.rdb";
		fb.dump_to_path = "data/dump3.txt";
		fb.fp = fopen(fb.dump_to_path, "w");
		fb.total = 0;

		tmstart2 = time(NULL);

		fprintf(fb.fp, "version:%d\n", rp->version);
		fprintf(fb.fp, "==== dump start ====\n\n");

		rdb_parse_file(rp, fb.path);

		fprintf(fb.fp, "==== dump over ====\n");
		fprintf(fb.fp, "chksum:%lld\n", rp->chksum);
		fprintf(fb.fp, "bytes:%lld\n", rp->parsed);
		fprintf(fb.fp, "nodes:%d\n", fb.total);

		tmover2 = time(NULL);
		fprintf(fb.fp, "cost: %lld seconds", tmover2 - tmstart2);
		fclose(fb.fp);

		reset_rdb_parser(rp);
	}

	tmover = time(NULL);
    printf("total = %lld, per_time_cost = %f",
        (tmover - tmstart), (float)(tmover - tmstart) / count);
    destroy_rdb_parser(rp);
    return 0;
}