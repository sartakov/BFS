#define FUSE_USE_VERSION 30

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <pthread.h>

#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include <hiredis.h>

#define CHUNK_SIZE	4096

#include "redis.c"

redisContext *c;
//redisReply *reply;

pthread_mutex_t read_lock;
pthread_mutex_t write_lock;


static redisContext * connect(char *hostname, int port)
{
    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout(hostname, port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    return c;
}


static int bfs_getattr(const char *path, struct stat *stbuf)
{
	int res = 0;
	redisReply *reply;

	pthread_mutex_lock(&read_lock);

	memset(stbuf, 0, sizeof(struct stat));
	if (strcmp(path, "/") == 0) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
	} else {
		reply = get_attr(c, path);

		if( reply->str==NULL) {
			res=-ENOENT;
			freeReplyObject(reply);
			pthread_mutex_unlock(&read_lock);
			return res;
		}

		stbuf->st_nlink = 1;

		stbuf->st_mode = atoi(reply->str);

		if (stbuf->st_mode & S_IFDIR) {
			stbuf->st_nlink = 2;
		}

		freeReplyObject(reply);

		reply = get_size(c, path);
		stbuf->st_size = atoi(reply->str);
		freeReplyObject(reply);

		reply = get_uid(c, path);
		stbuf->st_uid = atoi(reply->str);
		freeReplyObject(reply);

		reply = get_gid(c, path);
		stbuf->st_gid = atoi(reply->str);
		freeReplyObject(reply);
	} 

pthread_mutex_unlock(&read_lock);
	return res;
}

static int bfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{
	(void) offset;
	(void) fi;
	size_t i;

	redisReply *reply;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	pthread_mutex_lock(&read_lock);

	if (strcmp(path, "/") != 0)
		reply = get_names1(c,path);
	else
		reply = get_names2(c,path);

        for(i=0;i<reply->elements;i++) {
		if (strstr(reply->element[i]->str+strlen(path)+1, "/"))
			continue;

		filler(buf,reply->element[i]->str+strlen(path)+1*strcmp(path,"/"), NULL,0);
	}

	freeReplyObject(reply);

	pthread_mutex_unlock(&read_lock);
        return 0;
}

static int bfs_open(const char *path, struct fuse_file_info *fi)
{
// TODO
	pthread_mutex_lock(&read_lock);

#if 0

	redisReply *reply;
	if(strcmp(path, reply->str) != 0) {
		freeReplyObject(reply);
		return -ENOENT;
	}
	
	if ((fi->flags & 3) != O_RDONLY)
		return -EACCES;
#endif

	pthread_mutex_unlock(&read_lock);

        return 0;
}

static int just_write(const char *path, off_t offset, off_t size, char *buf)
{

	redisReply *reply,*r2;
	size_t len;


	unsigned long int chunk=offset/CHUNK_SIZE;
	off_t tmp_offset=offset-chunk*CHUNK_SIZE;
	size_t	wsize=0, tmpsize=0;

	long long node;
	node = get_node(c, path);

	while(size>0) {
		reply = get_chunk(c,node,chunk);
		len = reply->len;

		if(len==0) {  //create page if no exist
			char buff[CHUNK_SIZE];
			memset(buff,0,CHUNK_SIZE);
			free(reply->str);
			reply->str=buff;
			len=CHUNK_SIZE;
		}

		if (tmp_offset!=0) {
			if (tmp_offset + size > len)
			    tmpsize = len - tmp_offset;
			else
			    tmpsize=size;
//			if(buf==NULL)
				memset(reply->str + tmp_offset, 0, tmpsize);
//			else
//				memcpy(reply->str + tmp_offset, buf, tmpsize);
			tmp_offset=0;
		} else {
			if(size > len)
			    tmpsize = len;
			else
			    tmpsize=size;

//			if(buf==NULL)
				memset(reply->str, 0, tmpsize);
//			else
//				memcpy(reply->str, buf, tmpsize);
		}

		r2 = set_chunk(c, node, chunk, reply->str);

		freeReplyObject(reply);
		freeReplyObject(r2);

		buf=buf+tmpsize;
		wsize=wsize+tmpsize;
		size=size-tmpsize;
		chunk++;
	}

	return 0;

}


static int bfs_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;
	redisReply *reply;

	pthread_mutex_lock(&read_lock);

#if 0
#ifdef OLD_PROT
	reply = redisCommand(c,"GET %s#NAME", path);
#else
	reply = get_name(c, path);
#endif
	printf("value = %s\n", reply->str);
	if(strcmp(path, reply->str) != 0) {
		freeReplyObject(reply);
		return -ENOENT;
	}

	freeReplyObject(reply);
#endif

	reply = get_size(c, path);

	unsigned long int cur_size = atoi(reply->str);
	freeReplyObject(reply);

	if(offset>cur_size) { 
		pthread_mutex_unlock(&read_lock);
		return 0;
	}

	unsigned long int chunk=offset/CHUNK_SIZE;
	off_t tmp_offset=offset-chunk*CHUNK_SIZE;
	size_t	rsize=0, tmpsize=0;

	long long node;
	node = get_node(c, path);

	while(size>0) {
		reply = get_chunk(c, node, chunk);
		len = reply->len;

//FIXME, why!?!??!
		if(len==0) {  //create page if no exist
			char buff[CHUNK_SIZE];
			memset(buff,0,CHUNK_SIZE);
			free(reply->str);
			reply->str=buff;
			len=CHUNK_SIZE;
		}


		if(len!=CHUNK_SIZE) {
			printf("error %d:\t len = %d\tchunk = %lu, path = %s, offset = %llx, size = %x\n", __LINE__, len, chunk, path, offset, size);
			exit(0);
		}

		if (tmp_offset!=0) {
			if (tmp_offset + size > len)
				tmpsize = len - tmp_offset;
			else
				tmpsize=size;
			memcpy(buf, reply->str + tmp_offset, tmpsize);
			tmp_offset=0;
		} else {
			if(size > len)
				tmpsize = len;
			else
				tmpsize=size;
			memcpy(buf, reply->str, tmpsize);
		}

		buf=buf+tmpsize;
		rsize=rsize+tmpsize;
		size=size-tmpsize;
		offset=0;
		chunk++;

		freeReplyObject(reply);

	}

	pthread_mutex_unlock(&read_lock);
	size=rsize;
	return rsize;
}
static int bfs_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
        (void) fi;
	size_t len;
	redisReply *reply, *r2;

	pthread_mutex_lock(&read_lock);

#if 0 // TODO
	reply = get_name(c, path);

	if(strcmp(path, reply->str) != 0) {
		freeReplyObject(reply);
		return -ENOENT;
	}
	freeReplyObject(reply);
#endif

	reply = get_size(c, path);

	int cur_size = atoi(reply->str);
	freeReplyObject(reply);

	if(offset> cur_size) {
		just_write(path, offset, size, NULL);
	}

	int new_size=cur_size;
	
	if (offset+size > cur_size)
		new_size=offset+size;

	unsigned long int chunk=offset/CHUNK_SIZE;
	off_t tmp_offset=offset-chunk*CHUNK_SIZE;
	size_t	wsize=0, tmpsize=0;

	long long node;
	node = get_node(c, path);

	while(size>0) {
		reply = get_chunk(c, node, chunk);

		len = reply->len;

		if(len==0) {  //create page if no exist
			char buff[CHUNK_SIZE];
			memset(buff,0,CHUNK_SIZE);
			free(reply->str);
			reply->str=buff;
			len=CHUNK_SIZE;
		}

		if (tmp_offset!=0) {
			if (tmp_offset + size > len)
			    tmpsize = len - tmp_offset;
			else
			    tmpsize=size;
			memcpy(reply->str + tmp_offset, buf, tmpsize);
			tmp_offset=0;
		} else {
			if(size > len)
			    tmpsize = len;
			else
			    tmpsize=size;
			memcpy(reply->str, buf, tmpsize);
		}

		r2=set_chunk(c, node, chunk, reply->str);
		freeReplyObject(reply);
		freeReplyObject(r2);
		buf=buf+tmpsize;
		wsize=wsize+tmpsize;
		size=size-tmpsize;
		chunk++;
	}
	reply = set_size(c, path, new_size);
	freeReplyObject(reply);
	size=wsize;
	pthread_mutex_unlock(&read_lock);
        return wsize;
}



static int bfs_truncate(const char *path, off_t size)
{
	redisReply *reply;
	int cur_size=0;

	pthread_mutex_lock(&read_lock);

	reply = get_size(c, path);

	if(reply->str!=NULL) {
	// file exists
		cur_size = atoi(reply->str);
		freeReplyObject(reply);
		
		if(size > cur_size ) {
		    // extand 
			just_write(path, cur_size, size-cur_size, NULL);
		} else {
			just_write(path, size, cur_size-size, NULL);
		}

		reply = set_size(c, path, size);
		freeReplyObject(reply);

		
	} else {
	// new file
		freeReplyObject(reply);

		reply = set_size(c, path, size);
		freeReplyObject(reply);

//		if(size == 0) { //zero sized file just title, no page
//			goto exit;
//		} 

		char buff[CHUNK_SIZE];
		memset(buff,0,CHUNK_SIZE);

		long long node;
		node = get_node(c, path);

		reply = set_chunk(c, node, cur_size/CHUNK_SIZE, buff);
		freeReplyObject(reply);

	}

//exit:
	pthread_mutex_unlock(&read_lock);

        return 0;
}


static int bfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
	redisReply *reply;

	pthread_mutex_lock(&read_lock);

	reply = set_size(c, path, 0);
	freeReplyObject(reply);

	reply = set_attr(c, path, mode);
	freeReplyObject(reply);

	long long node = get_free_node(c);

	reply = set_node(c, path, node);
	freeReplyObject(reply);

	uid_t uid=getuid();
	gid_t gid=getgid();

	reply = set_uid(c, path, uid);
	freeReplyObject(reply);

	reply = set_gid(c, path, gid);
	freeReplyObject(reply);

	pthread_mutex_unlock(&read_lock);

        return 0;
}

static int bfs_mkdir(const char *path, mode_t mode)
{
	redisReply *reply;

	pthread_mutex_lock(&read_lock);

	reply = set_attr(c, path, mode | S_IFDIR);
	freeReplyObject(reply);

	reply = set_size(c, path, 0);
	freeReplyObject(reply);

	long long node = get_free_node(c);

	reply = set_node(c, path, node);
	freeReplyObject(reply);

	uid_t uid=getuid();
	gid_t gid=getgid();


	reply = set_uid(c, path, uid);
	freeReplyObject(reply);

	reply = set_gid(c, path, gid);
	freeReplyObject(reply);


	pthread_mutex_unlock(&read_lock);

        return 0;
}

static int bfs_unlink(const char *path)
{

	redisReply *reply;

	pthread_mutex_lock(&read_lock);

	long long node;
	node = get_node(c, path);

	reply = del_name(c, path);
	freeReplyObject(reply);

	if(node!=0) {
		reply = del_node(c, node);
		freeReplyObject(reply);
	}

	pthread_mutex_unlock(&read_lock);

        return 0;
}

static int bfs_rmdir(const char *path)
{
	unsigned long int i;

	redisReply *reply,*r2;

	pthread_mutex_lock(&read_lock);

	reply = get_names1(c, path);

        for(i=0;i<reply->elements;i++) {
		long long node;
		node = get_node(c, reply->element[i]->str);

		r2 = del_name(c, reply->element[i]->str);
		freeReplyObject(r2);

		r2 = del_node(c, node);
		freeReplyObject(r2);
	}

	freeReplyObject(reply);

	r2 = del_name(c, path);
	freeReplyObject(r2);

	pthread_mutex_unlock(&read_lock);
        return 0;
}

static int bfs_chmod(const char *path, mode_t mode)
{
	redisReply *reply;

	pthread_mutex_lock(&read_lock);

	reply = set_attr(c, path, mode);
	freeReplyObject(reply);

	pthread_mutex_unlock(&read_lock);
	
	return 0;
}

static int bfs_chown(const char *path, uid_t uid, gid_t gid)
{
	redisReply *reply;

	pthread_mutex_lock(&read_lock);

	reply = set_uid(c, path, uid);
	freeReplyObject(reply);

	reply = set_gid(c, path, gid);
	freeReplyObject(reply);

	pthread_mutex_unlock(&read_lock);

        return 0;
}

static int bfs_symlink(const char *from, const char *to)
{
	redisReply *reply;

	printf("from = %s\t to = %s\n", from, to);

#if 1
	pthread_mutex_lock(&read_lock);
	reply = set_attr(c, to, S_IFLNK);

	freeReplyObject(reply);
	reply = set_size(c, to, 7);
	freeReplyObject(reply);

	uid_t uid=getuid();
	gid_t gid=getgid();

	reply = set_uid(c, to, uid);
	freeReplyObject(reply);

	reply = set_gid(c, to, gid);
	freeReplyObject(reply);

	reply = set_symlink(c, to, from);
	freeReplyObject(reply);

	pthread_mutex_unlock(&read_lock);
#endif

        return 0;
}

static int bfs_readlink(const char *path, char *buf, size_t size)
{
#if 1
	redisReply *reply;

	pthread_mutex_lock(&read_lock);

	reply = get_symlink(c, path);
	size = reply->len;
	memcpy(buf, reply->str, reply->len);
	freeReplyObject(reply);

	pthread_mutex_unlock(&read_lock);
#endif

        return 0;
}


static int bfs_rename(const char *from, const char *to)
{
//TODO: check if exists
	pthread_mutex_lock(&read_lock);
	redisReply *reply, *r2;
	unsigned long int i;
	reply = get_names1(c, from);

        for(i=0;i<reply->elements;i++) {
		char buf[CHUNK_SIZE];
		sprintf(buf, "%s%s", to, reply->element[i]->str+strlen(from));
		r2 = re_name(c, reply->element[i]->str, buf);
		freeReplyObject(r2);
	}

	freeReplyObject(reply);

	reply = re_name(c, from, to);
	freeReplyObject(reply);

	pthread_mutex_unlock(&read_lock);

        return 0;
}

/***********************************************/

static int bfs_access(const char *path, int mask)
{
	printf("not implemented  %s\n",__func__);
        return 0;
}


static int bfs_link(const char *from, const char *to)
{
	printf("not implemented  %s\n",__func__);
        return 0;
}



#ifdef HAVE_UTIMENSAT
static int bfs_utimens(const char *path, const struct timespec ts[2])
{
	printf("not implemented  %s\n",__func__);
        return 0;
}
#endif


static int bfs_statfs(const char *path, struct statvfs *stbuf)
{
	printf("not implemented  %s\n",__func__);
        return 0;
}
static int bfs_release(const char *path, struct fuse_file_info *fi)
{
/* Just a stub.  This method is optional and can safely be left
             unimplemented */

        return 0;
}
static int bfs_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
/* Just a stub.  This method is optional and can safely be left
             unimplemented */
        return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
static int bfs_fallocate(const char *path, int mode,
                        off_t offset, off_t length, struct fuse_file_info *fi)
{
	printf("not implemented  %s\n",__func__);
        return res;
}
#endif

#ifdef HAVE_SETXATTR

static int bfs_setxattr(const char *path, const char *name, const char *value,
                        size_t size, int flags)
{
	printf("not implemented  %s\n",__func__);
        return 0;
}
static int bfs_getxattr(const char *path, const char *name, char *value,
                        size_t size)
{
	printf("not implemented  %s\n",__func__);
        return res;
}
static int bfs_listxattr(const char *path, char *list, size_t size)
{
	printf("not implemented  %s\n",__func__);

        return res;
}
static int bfs_removexattr(const char *path, const char *name)
{
	printf("not implemented  %s\n",__func__);
        return 0;
}
#endif /* HAVE_SETXATTR */

static struct fuse_operations bfs_oper = {
        .getattr        = bfs_getattr,
        .access         = bfs_access,
        .readlink       = bfs_readlink,
        .readdir        = bfs_readdir,
        .mknod          = bfs_mknod,
        .mkdir          = bfs_mkdir,
        .symlink        = bfs_symlink,
        .unlink         = bfs_unlink,
        .rmdir          = bfs_rmdir,
        .rename         = bfs_rename,
        .link           = bfs_link,
        .chmod          = bfs_chmod,
        .chown          = bfs_chown,
        .truncate       = bfs_truncate,
#ifdef HAVE_UTIMENSAT
        .utimens        = bfs_utimens,
#endif
        .open           = bfs_open,
        .read           = bfs_read,
        .write          = bfs_write,
        .statfs         = bfs_statfs,
        .release        = bfs_release,
        .fsync          = bfs_fsync,
#ifdef HAVE_POSIX_FALLOCATE
        .fallocate      = bfs_fallocate,
#endif
#ifdef HAVE_SETXATTR
        .setxattr       = bfs_setxattr,
        .getxattr       = bfs_getxattr,
        .listxattr      = bfs_listxattr,
        .removexattr    = bfs_removexattr,
#endif
};

int main(int argc, char *argv[])
{
	c=connect("localhost",6379);

	redisReply *reply;

	reply = flushdb(c);

	printf("flushdb: %s\n", reply->str);
	freeReplyObject(reply);

	bfs_mknod("/test1.txt", 0000 | S_IFREG , 2);
	bfs_mknod("/test2.txt", 0111 | S_IFREG , 2);
	bfs_mknod("/test3.txt", 0777 | S_IFREG , 2);

	bfs_write("/test1.txt", "text1 inside\n", sizeof("text1 inside\n"), 0, NULL);
	bfs_write("/test2.txt", "text2 inside\n", sizeof("text2 inside\n"), 0, NULL);
	bfs_write("/test3.txt", "text3 inside\n", sizeof("text3 inside\n"), 0, NULL);

        return fuse_main(argc, argv, &bfs_oper, NULL);
}
