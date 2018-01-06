using System;
using System.Linq;
using Shuttle.Core.Contract;
using Shuttle.Core.Uris;

namespace Shuttle.Esb.RabbitMQ
{
    public class RabbitMQUriParser
    {
        internal const string Scheme = "rabbitmq";

        public RabbitMQUriParser(Uri uri)
        {
            Guard.AgainstNull(uri, "uri");

            if (!uri.Scheme.Equals(Scheme, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new InvalidSchemeException(Scheme, uri.ToString());
            }

            Password = "";
            Username = "";
            Port = uri.Port;
            Host = uri.Host;

            if (uri.UserInfo.Contains(':'))
            {
                Username = uri.UserInfo.Split(':').First();
                Password = uri.UserInfo.Split(':').Last();
            }

            switch (uri.Segments.Length)
            {
                case 2:
                {
                    VirtualHost = "/";
                    Queue = uri.Segments[1];
                    break;
                }
                case 3:
                {
                    VirtualHost = uri.Segments[1];
                    Queue = uri.Segments[2];
                    break;
                }
                default:
                {
                    throw new UriFormatException(string.Format(Esb.Resources.UriFormatException,
                        "rabbitmq://[username:password@]host:port/[vhost/]queue", uri));
                }
            }

            if (Host.Equals("."))
            {
                Host = "localhost";
            }

            var builder = new UriBuilder(uri)
            {
                Host = Host,
                Port = Port,
                UserName = Username,
                Password = Password,
                Path = VirtualHost == "/" ? $"/{Queue}" : $"/{VirtualHost}/{Queue}"
            };

            Uri = builder.Uri;

            Local = Uri.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase) || Uri.Host.Equals("127.0.0.1");

            var queryString = new QueryString(uri);

            SetPrefetchCount(queryString);
            SetDurability(queryString);
            SetPersistent(queryString);
        }

        public Uri Uri { get; }
        public bool Local { get; }
        public bool Durable { get; private set; }
        public bool Persistent { get; private set; }

        public int PrefetchCount { get; private set; }
        public string Username { get; }
        public string Password { get; }
        public string Host { get; }
        public int Port { get; }
        public string VirtualHost { get; }
        public string Queue { get; }

        private void SetPrefetchCount(QueryString queryString)
        {
            PrefetchCount = 0;

            var parameter = queryString["prefetchCount"];

            if (parameter == null)
            {
                return;
            }

            if (ushort.TryParse(parameter, out var result))
            {
                PrefetchCount = result;
            }
        }

        private void SetDurability(QueryString queryString)
        {
            Durable = true;

            var parameter = queryString["durable"];

            if (parameter == null)
            {
                return;
            }

            if (bool.TryParse(parameter, out var result))
            {
                Durable = result;
            }
        }

        private void SetPersistent(QueryString parameters)
        {
            Persistent = true;

            var parameter = parameters["persistent"];

            if (parameter == null)
            {
                return;
            }

            if (bool.TryParse(parameter, out var result))
            {
                Persistent = result;
            }
        }
    }
}