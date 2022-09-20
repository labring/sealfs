/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPLv2.
  See the file COPYING.
*/

/** @file
 *
 * minimal example filesystem using high-level API
 *
 * Compile with:
 *
 *     gcc -Wall hello.c `pkg-config fuse3 --cflags --libs` -o hello
 *
 * ## Source code ##
 * \include hello.c
 */


#include <client/fuse_version.hpp>

#include <client/client.hpp>
#include <client/logging.hpp>

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <assert.h>

/*
 * Command line options
 *
 * We can't set default values for the char* fields here because
 * fuse_opt_parse would attempt to free() them when the user specifies
 * different values on the command line.
 */
static struct options {
	const char *filename;
	const char *contents;
	int show_help;
} options;

#define OPTION(t, p)                           \
    { t, offsetof(struct options, p), 1 }
static const struct fuse_opt option_spec[] = {
	OPTION("--name=%s", filename),
	OPTION("--contents=%s", contents),
	OPTION("-h", show_help),
	OPTION("--help", show_help),
	FUSE_OPT_END
};

static void *init(struct fuse_conn_info *conn,
			struct fuse_config *cfg)
{
	(void) conn;
	cfg->kernel_cache = 0; // do flush file data to disk
	return NULL;
}

/* Get file attributes from remote server */

static int lst_getattr(const char *path, struct stat *stbuf,
              struct fuse_file_info *fi)
{
	LOG("getattr %s", path);
    (void) fi;

    return get_client()->get_remote_file_attr(path, stbuf);
}

/* Read directory from remote server */

static int lst_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
              off_t offset, struct fuse_file_info *fi,
              enum fuse_readdir_flags flags)
{
	LOG("lst_readdir: %s", path);
    (void) offset;
    (void) fi;
    (void) flags;

    return get_client()->read_remote_dir(path, buf, filler);
}

/* Open file from remote server */

static int lst_open(const char *path, struct fuse_file_info *fi)
{
	LOG("lst_open: %s", path);
    return get_client()->open_remote_file(path, fi);
}

/* Read file from remote server */

static int lst_read(const char *path, char *buf, size_t size, off_t offset,
           struct fuse_file_info *fi)
{
	LOG("lst_read: %s", path);
    (void) fi;
    return get_client()->read_remote_file(path, buf, size, offset);
}

/* Write file to remote server */

static int lst_write(const char *path, const char *buf, size_t size,
            off_t offset, struct fuse_file_info *fi)
{
	LOG("lst_write: %s", path);
    (void) fi;
    return get_client()->write_remote_file(path, buf, size, offset);
}

/* Create file on remote server */

static int lst_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	LOG("lst_create: %s", path);
    (void) fi;
    return get_client()->create_remote_file(path, mode);
}

static int lst_mkdir(const char *path, mode_t mode)
{
	LOG("lst_mkdir: %s", path);
    return get_client()->create_remote_dir(path, mode);
}

struct fuse_operations lst_oper;

int main(int argc, char *argv[])
{
	int ret;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	/* Set defaults -- we have to use strdup so that
	   fuse_opt_parse can free the defaults if other
	   values are specified */
	options.filename = strdup("hello");
	options.contents = strdup("Hello World!\n");

    lst_oper.init		= init,
    lst_oper.getattr	= lst_getattr,
    lst_oper.readdir	= lst_readdir,
    lst_oper.open		= lst_open,
    lst_oper.read		= lst_read,
    lst_oper.write		= lst_write,
    lst_oper.create		= lst_create,
    lst_oper.mkdir		= lst_mkdir;
	
	init_logger("/home/luan/log.txt");

	LOG("Starting client");

	/* Parse options */
	if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1)
		return 1;

	/* When --help is specified, first print our own file-system
	   specific help text, then signal fuse_main to show
	   additional help (by adding `--help` to the options again)
	   without usage: line (by setting argv[0] to the empty
	   string) */
	if (options.show_help) {
		//show_help(argv[0]);
		assert(fuse_opt_add_arg(&args, "--help") == 0);
		args.argv[0][0] = '\0';
	}

	LOG("fuse_main");
	ret = fuse_main(args.argc, args.argv, &lst_oper, NULL);
	fuse_opt_free_args(&args);
	LOG("Client stopped");
	return ret;
}
