// get 
redisReply *get_attr(redisContext *c, const char *path) {
	return redisCommand(c,"HGET %s attr", path);
}

redisReply *get_size(redisContext *c, const char *path) {
	return redisCommand(c,"HGET %s size", path);
}

redisReply *get_uid(redisContext *c, const char *path) {
	return redisCommand(c,"HGET %s uid", path);
}

redisReply *get_gid(redisContext *c, const char *path) {
	return redisCommand(c,"HGET %s gid", path);
}

redisReply *get_symlink(redisContext *c, const char *path) {
	return redisCommand(c,"HGET %s symlink", path);
}


long long get_node(redisContext *c, const char *path) {
	redisReply *reply=redisCommand(c,"HGET %s node", path);
	if(reply->str==NULL) 
		return 0;
	long long node=atoll(reply->str);
	freeReplyObject(reply);
	return node;
}

redisReply *get_chunk(redisContext *c, long long node, unsigned long int chunk) {
	return redisCommand(c,"HGET %lld chunk%d", node,chunk);
}

redisReply *get_names1(redisContext *c, const char *path) {
	return redisCommand(c,"KEYS %s/*",path);
}

redisReply *get_names2(redisContext *c, const char *path) {
	return redisCommand(c,"KEYS /*",path);
}


// set

redisReply *set_attr(redisContext *c, const char *path, mode_t mode) {
	return redisCommand(c,"HSET %s attr %d", path, mode);
}

redisReply *set_size(redisContext *c, const char *path, unsigned long int size) {
	return redisCommand(c,"HSET %s size %d", path, size);
}

redisReply *set_uid(redisContext *c, const char *path, uid_t uid) {
	return redisCommand(c,"HSET %s uid %d", path, uid);
}

redisReply *set_gid(redisContext *c, const char *path, gid_t gid) {
	return redisCommand(c,"HSET %s gid %d", path, gid);
}

redisReply *set_symlink(redisContext *c, const char *path, const char *symlink) {
	return redisCommand(c,"HSET %s symlink %s", path, symlink);
}


redisReply *set_node(redisContext *c, const char *path, long long node) {
    	return redisCommand(c,"HSET %s node %lld", path, node);
}

redisReply *set_chunk(redisContext *c, long long node, unsigned long int chunk, char *buff) {
	return redisCommand(c,"HSET %lld chunk%d %b", node, chunk,buff, CHUNK_SIZE);
}

// del

redisReply *del_attr(redisContext *c, const char *path) {
	return redisCommand(c,"HDEL %s attr", path);
}

redisReply *del_size(redisContext *c, const char *path) {
	return redisCommand(c,"HDEL %s size", path);
}

redisReply *del_uid(redisContext *c, const char *path) {
	return redisCommand(c,"HDEL %s uid", path);
}

redisReply *del_gid(redisContext *c, const char *path) {
	return redisCommand(c,"HDEL %s gid", path);
}


redisReply *del_name(redisContext *c, const char *path) {
	return redisCommand(c,"DEL %s", path);
}

redisReply *del_chunk(redisContext *c, long long node, unsigned long int chunk) {
	return redisCommand(c,"HDEL %lld chunk%d", node,chunk);
}

redisReply *del_node(redisContext *c, long long node) {
	return redisCommand(c,"DEL %lld ", node);
}



// mgmt

redisReply *flushdb(redisContext *c) {
	return redisCommand(c, "flushdb");
}

long long get_free_node(redisContext *c) {
	redisReply *reply = redisCommand(c, "INCR free_node=1000");
	long long ret = reply->integer;
	freeReplyObject(reply);
	return ret;
}

redisReply *re_name(redisContext *c, const char *from, const char *to) {
	return redisCommand(c,"RENAME %s %s", from, to);
}