#include "agent.h"

#include <unistd.h>
#include <sys/wait.h>

#include "agt_utility.h"

#define DEFAULT_BUFFER_ENLARGE	1024

static void read_pipe(int *pfd, StringInfo buf);

/*
 * return exec's return value
 * when not eque 0, out save stderr
 */
int exec_shell(const char *exec, StringInfo out)
{
	StringInfoData buf_stdout;
	StringInfoData buf_stderr;
	volatile pid_t pid = 0;
	volatile int pipe_stdout[2] = {-1, -1};
	volatile int pipe_stderr[2] = {-1, -1};
	int exit_status;
	AssertArg(exec && out);

	PG_TRY();
	{
		if(pipe((int*)pipe_stdout) != 0
			|| pipe((int*)pipe_stderr) != 0)
		{
			ereport(ERROR, (errmsg("pipe for execute failed:%m")));
		}
		pid = fork();
		if(pid == 0)
		{
			/* in child close unused read end*/
			close(pipe_stdout[0]);
			close(pipe_stderr[0]);
			/* an dup to stdout and stderr */
			dup2(pipe_stdout[1], 1);
			dup2(pipe_stderr[1], 2);
			close(pipe_stdout[1]);
			close(pipe_stderr[1]);
			execl("/bin/sh", "sh", "-c", exec, NULL);
			/* run to here, we got an error */
			fprintf(stderr, "can not execute \"%s\":%m", exec);
			exit(EXIT_FAILURE);
		}else if(pid > 0)
		{
			int rval,maxfd;
			fd_set rfd;
			/* in parent close unused write end */
			close(pipe_stdout[1]);
			close(pipe_stderr[1]);
			pipe_stdout[1] = pipe_stderr[1] = -1;
			initStringInfo(&buf_stdout);
			initStringInfo(&buf_stderr);
			while(pipe_stdout[0] != -1 || pipe_stderr[0] != -1)
			{
				FD_ZERO(&rfd);
				maxfd = -1;
				if(pipe_stdout[0] != -1)
				{
					FD_SET(pipe_stdout[0], &rfd);
					maxfd = pipe_stdout[0];
				}
				if(pipe_stderr[0] != -1)
				{
					FD_SET(pipe_stderr[0], &rfd);
					if(maxfd < pipe_stderr[0])
						maxfd = pipe_stderr[0];
				}
				rval = select(maxfd+1, &rfd, NULL, NULL, NULL);
				if(rval < 0)
				{
					CHECK_FOR_INTERRUPTS();
					if(errno == EINTR)
						continue;
				}
				if(pipe_stdout[0] != -1 && FD_ISSET(pipe_stdout[0], &rfd))
					read_pipe((int*)&pipe_stdout[0], &buf_stdout);
				if(pipe_stderr[0] != -1 && FD_ISSET(pipe_stderr[0], &rfd))
					read_pipe((int*)&pipe_stderr[0], &buf_stderr);
			}
			/* get exit code */
			while(waitpid(pid, &exit_status, WUNTRACED) < 0 && errno == EINTR)
				; /* nothing todo */
		}else if(pid < 0)
		{
			ereport(ERROR, (errmsg("can not fork process:%m")));
		}
	}PG_CATCH();
	{
		if(pipe_stdout[0] != -1)
			close(pipe_stdout[0]);
		if(pipe_stdout[1] != -1)
			close(pipe_stdout[1]);
		if(pipe_stderr[0] != -1)
			close(pipe_stderr[0]);
		if(pipe_stderr[1] != -1)
			close(pipe_stderr[1]);
		if(pid != 0)
		{
			while(waitpid(pid, &exit_status, WUNTRACED) < 0 && errno == EINTR)
				; /* nothing todo */
		}
		PG_RE_THROW();
	}PG_END_TRY();

	Assert(buf_stdout.data && buf_stderr.data);
	if(WIFEXITED(exit_status))
		exit_status = WEXITSTATUS(exit_status);
	else
		exit_status = 127;
	if(exit_status == 0)
			appendBinaryStringInfo(out, buf_stdout.data, buf_stdout.len+1);
	else
		appendBinaryStringInfo(out, buf_stderr.data, buf_stderr.len+1);
	pfree(buf_stdout.data);
	pfree(buf_stderr.data);

	return exit_status;
}

static void read_pipe(int *pfd, StringInfo buf)
{
	int rval;
	AssertArg(pfd && buf && buf->data);
	if(buf->len >= buf->maxlen)
		enlargeStringInfo(buf, buf->maxlen+DEFAULT_BUFFER_ENLARGE);
	rval = read(*pfd, buf->data + buf->len, buf->maxlen-buf->len);
	if(rval == 0)
	{
		/* EOF */
		close(*pfd);
		*pfd = -1;
	}else if(rval < 0)
	{
		ereport(ERROR, (errmsg("read pipe failed:%m")));
	}else
	{
		buf->len += rval;
	}
}

