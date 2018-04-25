using System;
using System.IO;
using System.Threading.Tasks;

namespace Silk.FileContainers
{
	public interface IFileContainer
	{
		bool IsPubliclyAccessible { get; }
		Uri BaseUri { get; }
		Task<IFileUpload> StartUploadAsync(string storagePathAndFilename, Stream sourceStream, bool allowOverwrite = false);
		Task<IFileInfo> GetFileInfoAsync(string storagePathAndFilename);
		Task DeleteFileAsync(string storagePathAndFilename);
	}
}
