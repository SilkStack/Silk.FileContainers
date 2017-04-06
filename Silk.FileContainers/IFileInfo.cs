using System.IO;
using System.Threading.Tasks;

namespace Silk.FileContainers
{
	public interface IFileInfo
	{
		IFileContainer Container { get; }
		bool IsPubliclyAccessible { get; }
		bool Exists { get; }
		long FileLength { get; }
		string StoragePathAndFilename { get; }
		Task<Stream> GetReadStreamAsync();
	}
}
