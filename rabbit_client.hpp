#pragma once

#include <string>
#include <mutex>
#include <amqp.h>
#include <amqp_tcp_socket.h>

using namespace std;

__interface IErrorMessageHandler
{
    void OnError(const string& msg);
};

class RabbitClient
{
public:
	RabbitClient(IErrorMessageHandler *err_handler_, const string &exchange_){
		props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
		props.content_type = amqp_cstring_bytes("text/plain");
		props.delivery_mode = 2; /* persistent delivery mode */

        exchange = exchange_;
        err_handler = err_handler_;
		socket = NULL;
        conn = NULL;
	};
	~RabbitClient() {
	};

    void reconnect() {

        err_handler->OnError("Connection lost. Reconnecting...");

        Destroy();

        if (connect()) {
            err_handler->OnError("Reconnected");
        }
    }

    void Destroy() {
        if (conn) {
            amqp_rpc_reply_t res;
            res = amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);

            if (res.reply_type != AMQP_RESPONSE_NORMAL) {
                err_handler->OnError("Destroy: " + getErrorText(res));
            }

            res = amqp_connection_close(conn, AMQP_REPLY_SUCCESS);

            if (res.reply_type != AMQP_RESPONSE_NORMAL) {
                err_handler->OnError("Destroy: " + getErrorText(res));
            }

            amqp_destroy_connection(conn);
            conn = NULL;
        }
    };

    bool connect() {
        conn = amqp_new_connection();

        if (!conn) {
            err_handler->OnError("Connect: Failed to make connection");
            return false;
        }

        socket = amqp_tcp_socket_new(conn);
        if (!socket) {
            err_handler->OnError("Connect: Failed to init socket");
            return false;
        }

        int status;
        struct timeval tval;
        struct timeval* tv;

        tv = &tval;
        if (timeout_sec != 0) {
            tv->tv_sec = timeout_sec;
            tv->tv_usec = 0;
        }
        else
            tv = NULL;

        status = amqp_socket_open_noblock(socket, ip.c_str(), port, tv);
        if (status) {
            err_handler->OnError("Connect: Failed to open socket");
            return false;
        }

        amqp_rpc_reply_t res = amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
            login.c_str(), password.c_str());

        if (res.reply_type != AMQP_RESPONSE_NORMAL) {
            err_handler->OnError("Connect: " + getErrorText(res));
            return false;
        }

        amqp_channel_open_ok_t* channel_ret;
        channel_ret = amqp_channel_open(conn, 1);

        if (!channel_ret) {
            err_handler->OnError("Connect: Failed to open channel");
            return false;
        }

        res = amqp_get_rpc_reply(conn);

        if (res.reply_type != AMQP_RESPONSE_NORMAL) {
            err_handler->OnError("Connect: " + getErrorText(res));
            return false;
        }

        return true;
    }

    bool Connect(const string& ip_, int port_, const string& login_, const string& password_, long timeout_sec_ = 0) {

        ip = ip_;
        port = port_;
        login = login_;
        password = password_;
        timeout_sec = timeout_sec_;

        return connect();
    }

    bool publish(const string& routingKey, const string& message) {

        if (!conn) {
            return false;
        }

        int res = amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange.c_str()),
            amqp_cstring_bytes(routingKey.c_str()), 0, 0,
            &props, amqp_cstring_bytes(message.c_str()));

        if (res < 0) {
            err_handler->OnError("Publish: " + string(amqp_error_string2(res)));

            return false;
        }

        return true;
    }

	bool Publish(const string &routingKey, const string &message) {
        lock_guard<mutex> guard(m_pub);

		try {
            if (!publish(routingKey, message)) {
                reconnect();

                publish(routingKey, message);
            }
		}
		catch (const exception& e) {
			string err(e.what());
			err_handler->OnError("Publish: " + err);

            return false;
		}

        return true;
	};

private:
        string getErrorText(amqp_rpc_reply_t x) const {
            char buff[255];
            int res;

            switch (x.reply_type) {
                case AMQP_RESPONSE_NORMAL:
                    break;

                case AMQP_RESPONSE_NONE:
                    res = snprintf(buff, sizeof(buff), "%s", "missing RPC reply type!");
                    break;

                case AMQP_RESPONSE_LIBRARY_EXCEPTION:
                    return string(amqp_error_string2(x.library_error));

                case AMQP_RESPONSE_SERVER_EXCEPTION:
                    switch (x.reply.id) {
                        case AMQP_CONNECTION_CLOSE_METHOD: {
                            amqp_connection_close_t* m =
                                (amqp_connection_close_t*)x.reply.decoded;
                            res = snprintf(buff, sizeof(buff), "server connection error %uh, message: %.*s",
                                    m->reply_code, (int)m->reply_text.len, (char*)m->reply_text.bytes);
                            break;
                        }
                case AMQP_CHANNEL_CLOSE_METHOD: {
                    amqp_channel_close_t* m = (amqp_channel_close_t*)x.reply.decoded;
                    res = snprintf(buff, sizeof(buff), "server channel error %uh, message: %.*s\n",
                        m->reply_code, (int)m->reply_text.len, (char*)m->reply_text.bytes);
                    break;
                }
            default:
                res = snprintf(buff, sizeof(buff), "unknown server error, method id 0x%08X\n",
                    x.reply.id);
                break;
            }
            break;
        }

        if (res >= 0 && res < sizeof(buff)) {
            return string(buff);
        }

        return string("");
    }

private:
	amqp_basic_properties_t props;
	string exchange;
	amqp_socket_t* socket;
	amqp_connection_state_t conn;
	IErrorMessageHandler *err_handler;
    mutex m_pub;

    string ip;
    int port;
    string login;
    string password;
    long timeout_sec;
};
