using System;
using System.IO;
using System.Threading.Tasks;

namespace Silk.FileContainers
{
	public class FilesystemContainer : IFileContainer
	{
		private string _storeDirectory;

		public bool IsPubliclyAccessible { get; }

		public Uri BaseUri { get; }

		public FilesystemContainer(string storeDirectory)
		{
			IsPubliclyAccessible = false;
			BaseUri = null;
			_storeDirectory = StandardizeStoreDirectory(storeDirectory);
		}

		public FilesystemContainer(string storeDirectory, Uri baseUri)
		{
			IsPubliclyAccessible = true;
			BaseUri = baseUri;
			_storeDirectory = StandardizeStoreDirectory(storeDirectory);
		}

		private string StandardizeStoreDirectory(string storeDirectory)
		{
			while (storeDirectory.EndsWith("/") || storeDirectory.EndsWith("\\"))
			{
				storeDirectory = storeDirectory.Substring(0, storeDirectory.Length - 1);
			}
			return storeDirectory;
		}

		public Task<IFileUpload> StartUploadAsync(string storagePathAndFilename, Stream sourceStream, bool allowOverwrite = false)
		{
			var fullFilePath = GetFullPath(storagePathAndFilename);
			var fileInfo = new FileInfo(fullFilePath);
			if (!fileInfo.Directory.Exists)
				fileInfo.Directory.Create();
			if (!allowOverwrite && File.Exists(fullFilePath))
				throw new IOException("File already exists.");
			var upload = new FilesystemUpload(storagePathAndFilename,
				fullFilePath, sourceStream, this
				);
			upload.Start();
			return Task.FromResult<IFileUpload>(upload);
		}

		public Task<IFileInfo> GetFileInfoAsync(string storagePathAndFilename)
		{
			var fullFilePath = GetFullPath(storagePathAndFilename);
			var fileInfo = new FileInfo(fullFilePath);
			fileInfo.Refresh();
			storagePathAndFilename = fileInfo.FullName.Substring(fileInfo.FullName.Length - storagePathAndFilename.Length);
			return Task.FromResult<IFileInfo>(new FilesystemFileInfo(this, fileInfo, storagePathAndFilename));
		}

		public Task DeleteFileAsync(string storagePathAndFilename)
		{
			var fullFilePath = GetFullPath(storagePathAndFilename);
			if (File.Exists(fullFilePath))
				File.Delete(fullFilePath);
			return Task.FromResult(true);
		}

		private string GetFullPath(string storagePathAndFilename)
		{
			return $"{_storeDirectory}/{storagePathAndFilename}";
		}

		private class FilesystemFileInfo : IFileInfo
		{
			private FileInfo _fileInfo;

			public IFileContainer Container { get; }

			public bool IsPubliclyAccessible => Container.IsPubliclyAccessible;

			public bool Exists => _fileInfo.Exists;

			public long FileLength => _fileInfo.Length;

			public string StoragePathAndFilename { get; }

			public FilesystemFileInfo(IFileContainer container, FileInfo fileInfo, string storagePathAndFilename)
			{
				_fileInfo = fileInfo;
				Container = container;
				StoragePathAndFilename = storagePathAndFilename;
			}

			public Task<Stream> GetReadStreamAsync()
			{
				if (!Exists)
					throw new FileNotFoundException("File doesn't exist.", _fileInfo.FullName);
				return Task.FromResult<Stream>(new FileStream(_fileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));
			}
		}

		private class FilesystemUpload : IFileUpload
		{
			private readonly Stream _sourceStream;
			private readonly string _destinationFullPath;
			private readonly IFileContainer _container;
			private readonly string _storagePathAndFilename;

			public Task<IFileInfo> Task { get; private set; }

			public IFileInfo Result => Task?.Result;

			public bool HasFinished => (Task?.IsCompleted ?? false) || (Task?.IsCanceled ?? false) || (Task?.IsFaulted ?? false);

			public FilesystemUpload(string storagePathAndFilename, string fullPath, Stream sourceStream, IFileContainer container)
			{
				_storagePathAndFilename = storagePathAndFilename;
				_destinationFullPath = fullPath;
				_sourceStream = sourceStream;
				_container = container;
			}

			public void Start()
			{
				Task = PerformUpload();
			}

			private async Task<IFileInfo> PerformUpload()
			{
				var buffer = new byte[8192];
				using (var destinationStream = new FileStream(_destinationFullPath, FileMode.Create, FileAccess.Write, FileShare.None))
				{
					while (true)
					{
						var readSize = buffer.Length;
						if (_sourceStream.Length - _sourceStream.Position < readSize)
							readSize = (int)(_sourceStream.Length - _sourceStream.Position);
						var readCount = await _sourceStream.ReadAsync(buffer, 0, readSize);
						if (readCount > 0)
							await destinationStream.WriteAsync(buffer, 0, readCount);
						else
							break;
					}
				}
				return await _container.GetFileInfoAsync(_storagePathAndFilename);
			}
		}
	}
}
