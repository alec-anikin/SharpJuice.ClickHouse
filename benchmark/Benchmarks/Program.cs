using BenchmarkDotNet.Running;

namespace Benchmarks;

class Program
{
    static async Task Main(string[] args)
    {
        if (args.Length > 1 && args[0] == "profile")
        {
            var instance = new InsertMemory();

            await instance.Setup();

            try
            {
                while (true)
                {
                    switch (args[1])
                    {
                        case "array":
                            await instance.Array();
                            break;
                        case "memory":
                            await instance.Memory();
                            break;
                        case "readonlymemory":
                            await instance.ReadonlyMemory();
                            break;

                        default:
                            throw new NotSupportedException($"{args[1]} not supported");
                    }

                    await Task.Delay(TimeSpan.FromMinutes(1));
                }
            }
            finally
            {
                instance.Cleanup();
            }
        }
        else
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}