using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication2
{
    unsafe class Program
    {
        static unsafe void Main(string[] args)
        {
            // hardcoded to my machine...
            var file = @"D:\Temp\data.txt";
            var outputFile = @"D:\Temp\summary.txt";

            var sw = Stopwatch.StartNew();
            var allStats = new FastRecordCollection();
            using (var mmf = MemoryMappedFile.CreateFromFile(file, FileMode.Open))
            using (var accessor = mmf.CreateViewAccessor())
            {
                byte* buffer = null;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref buffer);
                var len = new FileInfo(file).Length;
                var entries = len / 50;
#if DEBUG
                int count = 1; // easier to debug with only 1 thread
#else
                int count = 4;
#endif
                var threadStates = new ThreadState[count];
                for (int i = 0; i < count; i++)
                {
                    threadStates[i] = new ThreadState
                    {
                        Records = new FastRecordCollection(),
                        Start = buffer + i * (entries / count) * 50,
                        End = buffer + (i + 1) * (entries / count) * 50
                    };
                }

                threadStates[threadStates.Length - 1].End = buffer + len;

                Parallel.ForEach(threadStates, state =>
                {
                    while (state.Start != state.End)
                    {
                        int id;
                        long duration;
                        state.NativeRecord.Parse(state.Start, out id, out duration);
                        state.Start += 50;
                        state.Records.AddDuration(id, duration);

                    }
                });

                for (int i = 0; i < count; i++)
                {
                    foreach (var record in threadStates[i].Records.GetItems())
                    {
                        allStats.AddDuration(record.Id, record.DurationInTicks);

                    }
                }

            }


            using (var output = File.CreateText(outputFile))
            {
                foreach (var entry in allStats.GetItems())
                {
                    output.WriteLine($"{entry.Id:D10} {TimeSpan.FromTicks(entry.DurationInTicks):c}");
                }
            }

            Console.WriteLine("Elapsed: " + sw.Elapsed);

            //Console.ReadLine();

        }



        public class FastRecordCollection
        {
            private const int RAISE_BY = 16;
            private const int MAX_LIST_ITEMS = 99999999 >> RAISE_BY;
            private readonly int SLOTSIZE = (int)Math.Pow(2, RAISE_BY);
            private readonly long[][] _List = new long[MAX_LIST_ITEMS][];


            public IEnumerable<FastRecord> GetItems()
            {
                for (var i = 0; i < MAX_LIST_ITEMS; i++)
                {
                    if (_List[i] == null) continue;
                    var l = _List[i];
                    for (var j = 0; j < SLOTSIZE - 1; j++)
                    {
                        if (l[j] > 0)
                        {
                            int id = (i << RAISE_BY) + j;
                            yield return new FastRecord
                            {
                                DurationInTicks = l[j],
                                Id = id
                            };
                        }
                    }
                }
            }

            public void AddDuration(int id, long duration)
            {
                var slot = id >> RAISE_BY;
                var i = (id & (SLOTSIZE - 1));

                var items = _List[slot];
                if (items == null)
                {
                    items = new long[SLOTSIZE];
                    _List[slot] = items;
                }
                items[i] += duration;
            }


        }


        public struct FastRecord
        {
            public int Id;
            public long DurationInTicks;
        }


        public unsafe class NativeRecord
        {
            public void Parse(byte* buffer, out int id, out long duration)
            {
                duration = (ParseTime(buffer + 20) - ParseTime(buffer));
                id = ParseInt(buffer + 40, 8);
            }


            private static readonly int[] DaysToMonth365 = {
    0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
            private static readonly int[] DaysToMonth366 = {
    0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

            private const long TicksPerMillisecond = 10000;
            private const long TicksPerSecond = TicksPerMillisecond * 1000;
            private const long TicksPerMinute = TicksPerSecond * 60;
            private const long TicksPerHour = TicksPerMinute * 60;
            private const long TicksPerDay = TicksPerHour * 24;

            // faster if using member variables, since
            // they don't have to be allocated on the stack
            int year, month, day, hour, min, sec, y, n;
            int[] days;
            long totalSeconds;
            bool leap;

            private long ParseTime(byte* buffer)
            {
                year = ParseInt(buffer, 4);
                month = ParseInt(buffer + 5, 2);
                day = ParseInt(buffer + 8, 2);
                hour = ParseInt(buffer + 11, 2);
                min = ParseInt(buffer + 14, 2);
                sec = ParseInt(buffer + 17, 2);

                leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);

                days = leap ? DaysToMonth366 : DaysToMonth365;
                y = year - 1;
                n = y * 365 + y / 4 - y / 100 + y / 400 + days[month - 1] + day - 1;

                totalSeconds = (long)hour * 3600 + (long)min * 60 + sec;

                return n * TicksPerDay + totalSeconds * TicksPerSecond;
            }

            private int ParseInt(byte* buffer, int size)
            {
                unchecked
                {
                    var val = buffer[0] - '0';
                    for (int i = 1; i < size; i++)
                    {
                        val *= 10;
                        val += buffer[i] - '0';
                    }
                    return val;
                }
            }
        }

        public unsafe class ThreadState
        {
            public FastRecordCollection Records;
            public byte* Start;
            public byte* End;
            public NativeRecord NativeRecord = new NativeRecord();
        }
    }
}




