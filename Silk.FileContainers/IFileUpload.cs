using System.Threading.Tasks;

namespace Silk.FileContainers
{
	public interface IFileUpload
	{
		Task<IFileInfo> Task { get; }
		IFileInfo Result { get; }
		bool HasFinished { get; }
	}
}
