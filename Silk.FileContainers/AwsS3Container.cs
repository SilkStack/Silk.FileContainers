using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Threading;

namespace Silk.FileContainers
{
	public class AwsS3Container : IFileContainer
	{
		private readonly string _bucketName;
		private readonly RegionEndpoint _regionEndpoint;
		private readonly string _awsAccessKeyId;
		private readonly string _awsSecretAccessKey;
		private readonly S3StorageClass _storageClass;

		public bool IsPubliclyAccessible => BaseUri != null;

		public Uri BaseUri { get; }

		public AwsS3Container(string awsAccessKeyId, string awsSecretAccessKey, string bucketName, RegionEndpoint regionEndpoint,
			S3StorageClass storageClass)
		{
			_awsAccessKeyId = awsAccessKeyId;
			_awsSecretAccessKey = awsSecretAccessKey;
			_bucketName = bucketName;
			_regionEndpoint = regionEndpoint;
			_storageClass = storageClass;
		}

		public AwsS3Container(string awsAccessKeyId, string awsSecretAccessKey, string bucketName, RegionEndpoint regionEndpoint,
			S3StorageClass storageClass, Uri baseUri)
		{
			_awsAccessKeyId = awsAccessKeyId;
			_awsSecretAccessKey = awsSecretAccessKey;
			_bucketName = bucketName;
			_regionEndpoint = regionEndpoint;
			_storageClass = storageClass;
			BaseUri = baseUri;
		}

		private IAmazonS3 GetS3Client()
		{
			return new AmazonS3Client(_awsAccessKeyId, _awsSecretAccessKey, _regionEndpoint);
		}

		public Task DeleteFileAsync(string storagePathAndFilename)
		{
			using (var client = GetS3Client())
				return client.DeleteObjectAsync(_bucketName, storagePathAndFilename);
		}

		public async Task<IFileInfo> GetFileInfoAsync(string storagePathAndFilename)
		{
			using (var client = GetS3Client())
			{
				try
				{
					var s3Response = await client.GetObjectMetadataAsync(_bucketName, storagePathAndFilename);
					return new S3FileInfo(storagePathAndFilename, this, s3Response.ContentLength, _bucketName);
				}
				catch (AmazonS3Exception s3Ex)
				{
					if (s3Ex.StatusCode == System.Net.HttpStatusCode.NotFound)
						return new S3FileNotFound(storagePathAndFilename, this);
					throw;
				}
			}
		}

		public async Task<IFileUpload> StartUploadAsync(string storagePathAndFilename, Stream sourceStream, bool allowOverwrite = false)
		{
			if (!allowOverwrite)
			{
				var fileInfo = await GetFileInfoAsync(storagePathAndFilename);
				if (fileInfo.Exists)
					throw new IOException("File already exists.");
			}
			var upload = new S3FileUpload(storagePathAndFilename, sourceStream, this,
				_awsAccessKeyId, _awsSecretAccessKey, _bucketName,
				_regionEndpoint, IsPubliclyAccessible ? S3CannedACL.PublicRead : S3CannedACL.Private,
				_storageClass);
			upload.Start();
			return upload;
		}

		private class S3FileInfo : IFileInfo
		{
			private readonly AwsS3Container _container;
			private readonly string _bucketName;

			public IFileContainer Container => _container;

			public bool IsPubliclyAccessible => _container.IsPubliclyAccessible;

			public bool Exists => true;

			public long FileLength { get; }

			public string StoragePathAndFilename { get; }

			public S3FileInfo(string storagePathAndFilename, AwsS3Container container,
				long fileLength, string bucketName)
			{
				_container = container;
				FileLength = fileLength;
				StoragePathAndFilename = storagePathAndFilename;
				_bucketName = bucketName;
			}

			public async Task<Stream> GetReadStreamAsync()
			{
				var s3Client = _container.GetS3Client();
				var s3Stream = await s3Client.GetObjectStreamAsync(_bucketName, StoragePathAndFilename, new Dictionary<string, object>());
				return new S3StreamWrapper(s3Stream, s3Client);
			}
		}

		private class S3StreamWrapper : Stream
		{
			private IAmazonS3 _s3Client;
			private Stream _s3Stream;

			public S3StreamWrapper(Stream s3Stream, IAmazonS3 s3Client)
			{
				_s3Client = s3Client;
				_s3Stream = s3Stream;
			}

			public override bool CanRead => _s3Stream.CanRead;

			public override bool CanSeek => _s3Stream.CanSeek;

			public override bool CanWrite => _s3Stream.CanWrite;

			public override long Length => _s3Stream.Length;

			public override long Position { get => _s3Stream.Position; set { _s3Stream.Position = value; } }

			public override void Flush()
			{
				_s3Stream.Flush();
			}

			public override int Read(byte[] buffer, int offset, int count)
			{
				return _s3Stream.Read(buffer, offset, count);
			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				return _s3Stream.Seek(offset, origin);
			}

			public override void SetLength(long value)
			{
				_s3Stream.SetLength(value);
			}

			public override void Write(byte[] buffer, int offset, int count)
			{
				_s3Stream.Write(buffer, offset, count);
			}

			public override bool CanTimeout => _s3Stream.CanTimeout;

			public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
			{
				return _s3Stream.CopyToAsync(destination, bufferSize, cancellationToken);
			}

			protected override void Dispose(bool disposing)
			{
				base.Dispose(disposing);
				if (_s3Stream != null)
				{
					_s3Stream.Dispose();
					_s3Client.Dispose();
					_s3Stream = null;
					_s3Client = null;
				}
			}

			public override Task FlushAsync(CancellationToken cancellationToken)
			{
				return _s3Stream.FlushAsync(cancellationToken);
			}

			public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
			{
				return _s3Stream.ReadAsync(buffer, offset, count, cancellationToken);
			}

			public override int ReadByte()
			{
				return _s3Stream.ReadByte();
			}

			public override int ReadTimeout { get => _s3Stream.ReadTimeout; set => _s3Stream.ReadTimeout = value; }

			public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
			{
				return _s3Stream.WriteAsync(buffer, offset, count, cancellationToken);
			}

			public override void WriteByte(byte value)
			{
				_s3Stream.WriteByte(value);
			}

			public override int WriteTimeout { get => _s3Stream.WriteTimeout; set => _s3Stream.WriteTimeout = value; }
		}

		private class S3FileNotFound : IFileInfo
		{
			public IFileContainer Container { get; }

			public bool IsPubliclyAccessible => Container.IsPubliclyAccessible;

			public bool Exists => false;

			public long FileLength => 0;

			public string StoragePathAndFilename { get; }

			public S3FileNotFound(string storagePathAndFilename, IFileContainer container)
			{
				Container = container;
				StoragePathAndFilename = storagePathAndFilename;
			}

			public Task<Stream> GetReadStreamAsync()
			{
				throw new FileNotFoundException();
			}
		}

		private class S3FileUpload : IFileUpload
		{
			private readonly string _storagePathAndFilename;
			private readonly Stream _sourceStream;
			private readonly IFileContainer _container;
			private readonly string _awsAccessKeyId;
			private readonly string _awsSecretAccessKey;
			private readonly string _bucketName;
			private readonly RegionEndpoint _regionEndpoint;
			private readonly S3CannedACL _cannedACL;
			private readonly S3StorageClass _storageClass;

			public Task<IFileInfo> Task { get; private set; }

			public IFileInfo Result => Task?.Result;

			public bool HasFinished => (Task?.IsCompleted ?? false) || (Task?.IsCanceled ?? false) || (Task?.IsFaulted ?? false);

			public S3FileUpload(string storagePathAndFilename, Stream sourceStream, IFileContainer container,
				string awsAccessKeyId, string awsSecretAccessKey, string bucketName, RegionEndpoint regionEndpoint,
				S3CannedACL cannedACL, S3StorageClass storageClass)
			{
				_storagePathAndFilename = storagePathAndFilename;
				_sourceStream = sourceStream;
				_container = container;
				_awsAccessKeyId = awsAccessKeyId;
				_awsSecretAccessKey = awsSecretAccessKey;
				_bucketName = bucketName;
				_regionEndpoint = regionEndpoint;
				_cannedACL = cannedACL;
				_storageClass = storageClass;
			}

			public void Start()
			{
				Task = PerformUpload();
			}

			private async Task<IFileInfo> PerformUpload()
			{
				var uploadRequest = new TransferUtilityUploadRequest
				{
					BucketName = _bucketName,
					InputStream = _sourceStream,
					AutoCloseStream = false,
					AutoResetStreamPosition = false,
					Key = _storagePathAndFilename,
					CannedACL = _cannedACL,
					StorageClass = _storageClass
				};
				var transferUtility = new TransferUtility(_awsAccessKeyId, _awsSecretAccessKey, _regionEndpoint);
				await transferUtility.UploadAsync(uploadRequest);
				return await _container.GetFileInfoAsync(_storagePathAndFilename);
			}
		}
	}
}
