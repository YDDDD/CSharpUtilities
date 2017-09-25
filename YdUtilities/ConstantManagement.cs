using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace YdUtilities.Constants
{
    public static class ConstantManagement
    {
        public const int STATUS_OK = 0;
        public const int OK = 0;

        public const int ERROR = -1000;

        public const int DB_ERROR = -100;
        public const int DB_DUPLICATE = -101;
        public const int DB_BLOCKED = -102;
        public const int DB_OPEN_CONNECTION_FAIL = -199;

        public const int API_ERROR = -200;
        public const int API_REQUEST_FAIL = -201;

        public const int IO_ERROR = -300;
    }
}
