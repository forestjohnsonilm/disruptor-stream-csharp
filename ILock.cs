using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DisruptorTest
{
    public interface ILock
    {
        void WithLock(Action action);
    }
}
