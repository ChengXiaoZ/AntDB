#include "postgres_fe.h"

#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>

#include "msg_queue_pipe.h"
#include "adbloader_log.h"

static ssize_t readn (int fd, void *ptr, size_t n);
static ssize_t writen (int fd, const void *ptr, size_t n);

ssize_t
readn (int fd, void *buf, size_t n)
{
	size_t nleft;
	ssize_t nread;
	char *ptr;

	nleft = n;
	ptr = (char*)buf;
	while (nleft > 0)
	{
		if ((nread = read(fd, ptr, nleft)) < 0)
		{
			if (nleft == n)
			{
				fprintf(stderr, "read failed \n");
				return (-1);
			}
			else
			{
				fprintf(stderr, "read failed, but read some  \n");
				break;
			}
		}
		else if (nread == 0)
		{
			fprintf(stderr, "read failed, may be end of file \n");
			break;
		}
		nleft -= nread;
		ptr += nread;
	}
	return (n - nleft);
}

ssize_t
writen (int fd, const void *buf, size_t n)
{
	size_t nleft;
	ssize_t nwritten;
	char *ptr;

	nleft = n;
	ptr = (char*) buf;
	while (nleft > 0)
	{
		if ((nwritten = write (fd, ptr, nleft)) < 0)
		{
			if (nleft == n)
			{
				fprintf(stderr, "write failed \n");
				return (-1);
			}
			else
			{
				fprintf(stderr, "write failed , but has write in some \n");
				break;
			}
		}
		else if (nwritten == 0)
		{
			fprintf(stderr, "write failed, may be end of file \n");
			break;
		}
		nleft -= nwritten;
		ptr += nwritten;
	}
	return (n - nleft);
}

void
mq_pipe_init (MessageQueuePipe *queue, char *name)
{
	Assert(queue != NULL && name != NULL);

	if(pthread_mutex_init(&queue->read_queue_mutex, NULL) != 0)
	{
		fprintf(stderr, "Can not initialize mutex:%s\n", strerror(errno));
		exit(1);
	}

	if(queue->write_lock)
	{
		if(pthread_mutex_init(&queue->write_queue_mutex, NULL) != 0)
		{
			fprintf(stderr, "Can not initialize mutex:%s\n", strerror(errno));
			exit(1);
		}
	}

	queue->name = (char *)palloc0(strlen(name) + 1);
	sprintf(queue->name, "%s", name);
	queue->name[strlen(name)] = '\0';
	if(pipe(queue->fd) < 0)
	{
		fprintf(stderr, "create pipe error \n");
		exit(1);
	}

	fcntl(queue->fd[0], F_SETFL, O_NOATIME);

	return;
}

int
mq_pipe_put (MessageQueuePipe *queue, LineBuffer* lineBuffer)
{
	int num = 0;
 
	Assert(queue != NULL);

	if (queue->write_lock)
		pthread_mutex_lock(&queue->write_queue_mutex);
	num = writen(queue->fd[1], (void*)&lineBuffer, sizeof(LineBuffer*));
	if (queue->write_lock)
		pthread_mutex_unlock(&queue->write_queue_mutex);
	if (num > 0 && num != sizeof(LineBuffer*))
	{
		/* wtrite failed element->lineBuffer->data */
		ADBLOADER_LOG(LOG_ERROR,
							"[mq_pipe] put linebuff to pipe error, buffer data :%s",
							lineBuffer->data);
		return -1;
	}

	return num;
}

int
mq_pipe_put_element (MessageQueuePipe *queue, QueueElementPipe *element)
{
	int num;
	Assert(queue != NULL && element != NULL);
	if (queue->write_lock)
		pthread_mutex_lock(&queue->write_queue_mutex);
	num = writen(queue->fd[1], (void*)&element, sizeof(QueueElementPipe*));
	if (queue->write_lock)
		pthread_mutex_unlock(&queue->write_queue_mutex);
	if (num > 0 && num != sizeof(element))
	{
		/* wtrite failed element->lineBuffer->data */
	}
	return num;
}

int 
mq_pipe_put_batch (MessageQueuePipe *queue, LineBuffer ** lineBuffer, int size)
{
	QueueElementPipe *element;
	int flag;
	int num;
	Assert(queue != NULL && lineBuffer != NULL && size > 0);
	if (queue->write_lock)
		pthread_mutex_lock(&queue->write_queue_mutex);
	for(flag = 0; flag < size; flag++)
	{
		element = (QueueElementPipe *)palloc0(sizeof(QueueElementPipe));
		element->lineBuffer = lineBuffer[flag];
		num = writen(queue->fd[1], (void*)element, sizeof(element));
		if (num > 0 && num != sizeof(element))
		{
			/* wtrite failed element->lineBuffer->data */
		}
	}
	if (queue->write_lock)
		pthread_mutex_unlock(&queue->write_queue_mutex);
	return 1;
}

LineBuffer*
mq_pipe_poll (MessageQueuePipe *queue)
{
	int	num;
	LineBuffer* lineBuffer;
	pthread_mutex_lock(&queue->read_queue_mutex);
	num = readn(queue->fd[0], (void*)&lineBuffer, sizeof(LineBuffer*));
	pthread_mutex_unlock(&queue->read_queue_mutex);
	if( num > 0 && num != sizeof(LineBuffer*))
	{
		/* read failed ,some can't read */
		return NULL;
	}
	if (num < 0)
	{
		/*error*/
		return NULL;
	}
	Assert(num == sizeof(LineBuffer*));
	return lineBuffer;
}

void
mq_pipe_destory(MessageQueuePipe *queue)
{
	Assert(queue != NULL);

	close(queue->fd[0]);
	close(queue->fd[1]);

	pg_free(queue->name);
	queue->name = NULL;

	return;
}

