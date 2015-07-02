﻿using System;
using System.Collections.Specialized;
using System.Linq;
using System.Web;
using Shuttle.Core.Infrastructure;
using Shuttle.ESB.Core;

namespace Shuttle.ESB.RabbitMQ
{
	public class RabbitMQUriParser
	{
		internal const string SCHEME = "rabbitmq";

		public Uri Uri { get; private set; }
		public bool Local { get; private set; }
		public bool Durable { get; private set; }

		public RabbitMQUriParser(Uri uri)
		{
			Guard.AgainstNull(uri, "uri");

			if (!uri.Scheme.Equals(SCHEME, StringComparison.InvariantCultureIgnoreCase))
			{
				throw new InvalidSchemeException(SCHEME, uri.ToString());
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
						throw new UriFormatException(string.Format(ESBResources.UriFormatException,
																   "rabbitmq://[username:password@]host:port/[vhost/]queue", Uri));
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
				Path = VirtualHost == "/" ? string.Format("/{0}", Queue) : string.Format("/{0}/{1}", VirtualHost, Queue)
			};

			Uri = builder.Uri;

			Local = Uri.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase) || Uri.Host.Equals("127.0.0.1");

			var parameters = HttpUtility.ParseQueryString(uri.Query);

			SetPrefetchCount(parameters);
			SetDurability(parameters);
		}

		private void SetPrefetchCount(NameValueCollection parameters)
		{
			PrefetchCount = 0;

			var parameter = parameters.Get("prefetchCount");

			if (parameter == null)
			{
				return;
			}

			ushort result;

			if (ushort.TryParse(parameter, out result))
			{
				PrefetchCount = result;
			}
		}

		private void SetDurability(NameValueCollection parameters)
		{
			Durable = true;

			var parameter = parameters.Get("durable");

			if (parameter == null)
			{
				return;
			}

			bool result;

			if (bool.TryParse(parameter, out result))
			{
				Durable = result;
			}
		}

		public int PrefetchCount { get; private set; }
		public string Username { get; private set; }
		public string Password { get; private set; }
		public string Host { get; private set; }
		public int Port { get; private set; }
		public string VirtualHost { get; private set; }
		public string Queue { get; private set; }

	}
}