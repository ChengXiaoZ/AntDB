
#include "postgres.h"

#include <unistd.h>
#include <libssh2.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <sys/ioctl.h>

#include "lib/stringinfo.h"
#include "libpq/ip.h"
#include "mgr/mgr_cmds.h"

static int waitsocket(int socket_fd, LIBSSH2_SESSION *session)
{
    struct timeval timeout;
    int rc;
    fd_set fd;
    fd_set *writefd = NULL;
    fd_set *readfd = NULL;
    int dir;

    timeout.tv_sec = 0;
    timeout.tv_usec = 100*1000;	/* 0.1 second */

    FD_ZERO(&fd);

    FD_SET(socket_fd, &fd);

    /* now make sure we wait in the correct direction */
    dir = libssh2_session_block_directions(session);

    if(dir & LIBSSH2_SESSION_BLOCK_INBOUND)
        readfd = &fd;

    if(dir & LIBSSH2_SESSION_BLOCK_OUTBOUND)
        writefd = &fd;

    rc = select(socket_fd + 1, readfd, writefd, NULL, &timeout);

    return rc;
}

static pgsocket connect_host(const char *hostname, unsigned short port, StringInfo message)
{
	struct addrinfo *addr;
	struct addrinfo *addrs;
	struct addrinfo hint;
	pgsocket sock;
	char str_port[6];
	int ret;

	AssertArg(hostname && message);
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;
	hint.ai_flags = AI_PASSIVE;
	sprintf(str_port, "%u", port);

	ret = pg_getaddrinfo_all(hostname, str_port, &hint, &addrs);
	if(ret != 0)
	{
		appendStringInfo(message, "could not resolve \"%s\":%s", hostname, gai_strerror(ret));
		return PGINVALID_SOCKET;
	}

	for(addr = addrs; addr != NULL; addr = addr->ai_next)
	{
		int error = -1;
		int len;
		struct timeval tm;
		fd_set set;
		unsigned long ul = 1;

		sock = socket(addr->ai_family, SOCK_STREAM, 0);
		if(sock == PGINVALID_SOCKET)
		{
			if(addr->ai_next != NULL)
				continue;
			appendStringInfo(message, "could not create socket:%m");
			break;
		}

		ioctl(sock, FIONBIO, &ul);
		if(connect(sock, addr->ai_addr, addr->ai_addrlen) != 0)
		{
			tm.tv_sec = 3; // time out time = 3s
			tm.tv_usec = 0;
			len = sizeof(int);
			FD_ZERO(&set);
			FD_SET(sock, &set);

			if(select(sock + 1, NULL, &set, NULL, &tm) > 0)
			{
				getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, (socklen_t *)&len);
				if(error != 0)
				{
					ul = 0;
					ioctl(sock, FIONBIO, &ul);
					appendStringInfo(message, "could not connect to \"%s:%s\"", hostname, str_port);
					sock = PGINVALID_SOCKET;
					break;
				}
				else
				{
					ul = 0;
					ioctl(sock, FIONBIO, &ul);
					break;
				}
			}
			else
			{
				sock = PGINVALID_SOCKET;
			}

			ul = 0;
			ioctl(sock, FIONBIO, &ul);

			closesocket(sock);
			if(addr->ai_next == NULL)
			{
				appendStringInfo(message, "could not connect to \"%s:%s\"", hostname, str_port);
				break;
			}else
			{
				continue;
			}
		}

		ul = 0;
		ioctl(sock, FIONBIO, &ul);
		break;
	}

	pg_freeaddrinfo_all(AF_UNSPEC, addrs);
	return sock;
}

static LIBSSH2_SESSION* start_ssh(pgsocket sock, const char *username, const char *password, StringInfo message)
{
	LIBSSH2_SESSION* session;
	StringInfoData buf;
	int rc;
	static bool is_inited = false;
	AssertArg(sock != PGINVALID_SOCKET && username);
	if(password == NULL)
		password = "";

	if(is_inited == false)
	{
		rc = libssh2_init(0);
		if(rc != LIBSSH2_ERROR_NONE)
		{
			closesocket(sock);
			appendStringInfo(message, "libssh2 initialization failed (%d)\n", rc);
			return NULL;
		}
		is_inited = true;
	}

	/* Create a session instance */
	session = libssh2_session_init();
	if(session == NULL)
	{
		closesocket(sock);
		appendStringInfoString(message, "could not initialize ssh2 session");
		return NULL;
	}

	/* tell libssh2 we want it all done non-blocking */
	libssh2_session_set_blocking(session, 0);

	/* ... start it up. This will trade welcome banners, exchange keys,
	 * and setup crypto, compression, and MAC layers
	 */
	for(;;)
	{
		rc = libssh2_session_handshake(session, sock);
		if(rc != LIBSSH2_ERROR_EAGAIN)
			break;
	}
	if(rc != LIBSSH2_ERROR_NONE)
		goto start_ssh_end_;

	/* first try public key */
	initStringInfo(&buf);
	enlargeStringInfo(&buf, MAXPGPATH);
	if(get_home_path(buf.data) != false)
	{
		char *publickey;
		char *privatekey;
		buf.len = strlen(buf.data);
		if(buf.data[buf.len] != '/')
			appendStringInfoChar(&buf, '/');
		publickey = psprintf("%s%s", buf.data, ".ssh/id_rsa.pub");
		privatekey = psprintf("%s%s", buf.data, ".ssh/id_rsa");
		for(;;)
		{
			rc = libssh2_userauth_publickey_fromfile(session, username
				, publickey, privatekey, password ? password:"");
			if(rc != LIBSSH2_ERROR_EAGAIN)
				break;
		}
		pfree(privatekey);
		pfree(publickey);
	}else
	{
		rc = LIBSSH2_ERROR_EAGAIN;
	}
	pfree(buf.data);

	/* try use password */
	if(rc != LIBSSH2_ERROR_NONE)
	{
		for(;;)
		{
			rc = libssh2_userauth_password(session, username
				, password ? password : "");
			if(rc != LIBSSH2_ERROR_EAGAIN)
				break;
		}
	}
	if(rc != LIBSSH2_ERROR_NONE)
		goto start_ssh_end_;

start_ssh_end_:
	if(rc != LIBSSH2_ERROR_NONE)
	{
		char *errmsg;
		int errmsg_len;
		libssh2_session_last_error(session, &errmsg, &errmsg_len, 0);
		appendBinaryStringInfo(message, errmsg, errmsg_len);
		appendStringInfoChar(message, '\0');
		libssh2_session_disconnect(session, "");
		libssh2_session_free(session);
		closesocket(sock);
		return NULL;
	}
	return session;
}

static LIBSSH2_CHANNEL* ssh_open_channel(LIBSSH2_SESSION *session
	, StringInfo message, pgsocket sock)
{
	LIBSSH2_CHANNEL *channel;
	char *errmsg;
	int errmsg_len;
	AssertArg(session && message && sock != PGINVALID_SOCKET);

	for(;;)
	{
		channel = libssh2_channel_open_session(session);
		if(channel != NULL)
			break;
		if(libssh2_session_last_error(session, NULL, NULL, 0) == LIBSSH2_ERROR_EAGAIN)
		{
			waitsocket(sock, session);
			continue;
		}
		break;
	}
	if(channel == NULL)
	{
		libssh2_session_last_error(session, &errmsg, &errmsg_len, 0);
		appendBinaryStringInfo(message, errmsg, errmsg_len);
		appendStringInfoChar(message, '\0');
		return NULL;
	}
	return channel;
}

static LIBSSH2_CHANNEL* ssh_exec(LIBSSH2_SESSION *session
	, const char *command, StringInfo message, pgsocket sock)
{
	LIBSSH2_CHANNEL *channel;
	char *errmsg;
	int errmsg_len;
	int rc;
	AssertArg(session && command && message && sock != PGINVALID_SOCKET);

	channel = ssh_open_channel(session, message, sock);
	if(channel == NULL)
		return NULL;

	while( (rc = libssh2_channel_exec(channel, command)) ==
			LIBSSH2_ERROR_EAGAIN )
	{
		waitsocket(sock, session);
	}
	if(rc != LIBSSH2_ERROR_NONE)
	{
		libssh2_session_last_error(session, &errmsg, &errmsg_len, 0);
		appendBinaryStringInfo(message, errmsg, errmsg_len);
		appendStringInfoChar(message, '\0');
		libssh2_channel_free(channel);
		return NULL;
	}
	return channel;
}

static int ssh_get_channel_exit_msg(LIBSSH2_SESSION *session, LIBSSH2_CHANNEL *channel, StringInfo message, pgsocket sock)
{
	int rc;
	int rc_out;
	int rc_err;
	StringInfoData out;
	StringInfoData err;
	initStringInfo(&out);
	initStringInfo(&err);
	for(;;)
	{
		if(out.len >= out.maxlen)
			enlargeStringInfo(&out, out.maxlen+1024);
		rc_out = libssh2_channel_read(channel, out.data + out.len, out.maxlen - out.len);
		if(rc_out > 0)
			out.len += rc_out;
		if(err.len >= err.maxlen)
			enlargeStringInfo(&err, err.maxlen+1024);
		rc_err = libssh2_channel_read_stderr(channel
			, err.data + err.len, err.maxlen - err.len);
		if(rc_err > 0)
			err.len += rc_err;

		if(rc_out == LIBSSH2_ERROR_EAGAIN || rc_err == LIBSSH2_ERROR_EAGAIN)
		{
			waitsocket(sock, session);
			continue;
		}
		if(rc_out > 0 || rc_err > 0)
			continue;
		if(rc_out <= 0 && rc_err <= 0)
			break;
	}

	if(rc_out != 0 || rc_err != 0)
	{
		char *errmsg;
		int msg_len;
		libssh2_session_last_error(session, &errmsg, &msg_len, 0);
		appendBinaryStringInfo(message, errmsg, msg_len);
		appendStringInfoChar(message, '\0');
	}else
	{
		while( (rc = libssh2_channel_close(channel)) == LIBSSH2_ERROR_EAGAIN )
			waitsocket(sock, session);
	}

	/*if( rc == 0 )*/
	{
		int exitcode;
		size_t exitsignal_len;
		char *exitsignal;
		exitcode = libssh2_channel_get_exit_status( channel );
		libssh2_channel_get_exit_signal(channel, &exitsignal, &exitsignal_len
			, NULL, NULL, NULL, NULL);
		if(exitcode != 0)
		{
			appendStringInfoChar(&err, '\0');
			appendStringInfoString(message, err.data);
		}else
		{
			appendStringInfoChar(&out, '\0');
			appendStringInfoString(message, out.data);
		}
		pfree(out.data);
		pfree(err.data);
		if(exitsignal)
		{
			appendBinaryStringInfo(message, exitsignal, exitsignal_len);
			appendStringInfoChar(message, '\0');
			rc = -1;
		}else
		{
			rc = exitcode;
		}
	}
	return rc;
}

int ssh2_start_agent(const char *hostname,
							unsigned short port,
					 		const char *username,
					 		const char *password,
					 		const char *commandline,
					 		StringInfo message)
{
	pgsocket sock;
	LIBSSH2_SESSION *session;
	LIBSSH2_CHANNEL *channel;
	int rc;

	sock = connect_host(hostname, port, message);
	if(sock == PGINVALID_SOCKET)
		return -1;

	session = start_ssh(sock, username, password, message);
	if(session == NULL)
	{
		closesocket(sock);
		return -1;
	}

	channel = ssh_exec(session, commandline, message, sock);
	if(channel == NULL)
	{
		rc = -1;
		goto shutdown_;
	}

	rc = ssh_get_channel_exit_msg(session, channel, message, sock);
	if(rc == 127 && message->len == 0)
		appendStringInfo(message, _("%s: No such file"), commandline);
	libssh2_channel_free(channel);
	channel = NULL;

shutdown_:
	libssh2_session_disconnect(session, "");
	libssh2_session_free(session);

	closesocket(sock);

	/* remove last '\n' */
	while(message->len)
	{
		if(message->data[message->len-1] == '\n'
			|| message->data[message->len-1] == '\r')
		{
			-- (message->len);
		}else
		{
			break;
		}
	}
	if(message->len)
	{
		if(message->data[message->len] != '\0')
			appendStringInfoChar(message, '\0');
	}else
	{
		message->data[0] = '\0';
	}
	return rc;
}

bool ssh2_deplory_tar(const char *hostname,
							unsigned short port,
							const char *username,
							const char *password,
							const char *path,
							FILE *tar,
							StringInfo message)
{
	LIBSSH2_SESSION *session;
	LIBSSH2_CHANNEL *channel;
	StringInfoData buf;
	ssize_t cnt;
	pgsocket sock;
	int rc;

	if(fseek(tar, 0, SEEK_SET) < 0)
	{
		appendStringInfo(message, "Can not move tar file to start:%m");
		return false;
	}

	sock = connect_host(hostname, port, message);
	if(sock == PGINVALID_SOCKET)
		return false;

	session = start_ssh(sock, username, password, message);
	if(session == NULL)
	{
		closesocket(sock);
		return false;
	}

	rc = 0;
	initStringInfo(&buf);
	appendStringInfo(&buf, "mkdir -p \"%s\"", path);
	channel = ssh_exec(session, buf.data, message, sock);
	if(channel != NULL)
	{
		rc = ssh_get_channel_exit_msg(session, channel, message, sock);
		if(rc == 127 && message->len == 0)
			appendStringInfo(message, _("%s: No such file"), "mkdir");
		if(rc != 0)
			goto shutdown_;
		libssh2_channel_free(channel);
		resetStringInfo(&buf);
		appendStringInfo(&buf, "tar xC \"%s\"", path);
		channel = ssh_exec(session, buf.data, message, sock);
	}
	if(channel == NULL)
	{
		rc = -1;
		goto shutdown_;
	}

	/* send tar data */
	enlargeStringInfo(&buf, 8192);
	resetStringInfo(&buf);
	for(;;)
	{
		fd_set wfd;
		if(buf.cursor == buf.len)
		{
			resetStringInfo(&buf);
			cnt = fread(buf.data, 1, buf.maxlen, tar);
			if(cnt > 0)
				buf.len = cnt;
			else
				break;
		}
		cnt = libssh2_channel_write(channel, buf.data + buf.cursor, buf.len - buf.cursor);
		if(cnt < 0 && libssh2_channel_eof(channel))
			break;
		if(cnt == LIBSSH2_ERROR_EAGAIN)
		{
			FD_ZERO(&wfd);
			FD_SET(sock, &wfd);
			select(sock+1, NULL, &wfd, NULL, NULL);
			continue;
		}else if(cnt == LIBSSH2_ERROR_CHANNEL_CLOSED
			|| cnt == LIBSSH2_ERROR_CHANNEL_EOF_SENT)
		{
			break;
		}else if(cnt <= 0 )
		{
			break;
		}
		buf.cursor += cnt;
	}
	while(libssh2_channel_send_eof(channel) == LIBSSH2_ERROR_EAGAIN)
		waitsocket(sock, session);

	rc = ssh_get_channel_exit_msg(session, channel, message, sock);
	if(rc == 127 && message->len == 0)
		appendStringInfo(message, _("%s: No such file"), "mkdir or tar");
	libssh2_channel_free(channel);
	channel = NULL;

shutdown_:
	if(channel)
		libssh2_channel_free(channel);
	pfree(buf.data);
	libssh2_session_disconnect(session, "");
	libssh2_session_free(session);

	closesocket(sock);
	return rc == 0 ? true:false;
}
