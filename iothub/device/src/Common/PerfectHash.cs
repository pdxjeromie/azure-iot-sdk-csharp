// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.-

using System;
using System.Text;

namespace Microsoft.Azure.Devices.Common.Cloud
{
    internal static class PerfectHash
    {
        public static long HashToLong(string data)
        {
            uint hash1;
            uint hash2;

            PerfectHash.ComputeHash(Encoding.UTF8.GetBytes(data.ToUpper()), seed1: 0, seed2: 0, hash1: out hash1, hash2: out hash2);
            long hashedValue = ((long)hash1 << 32) | (long)hash2;

            return hashedValue;
        }

        public static short HashToShort(string data)
        {
            uint hash1;
            uint hash2;

            PerfectHash.ComputeHash(Encoding.UTF8.GetBytes(data.ToUpper()), seed1: 0, seed2: 0, hash1: out hash1, hash2: out hash2);
            long hashedValue = hash1 ^ hash2;

            return (short)hashedValue;
        }

        // Perfect hashing implementation. source: distributed cache team
        private static void ComputeHash(byte[] data, uint seed1, uint seed2, out uint hash1, out uint hash2)
        {
            uint a, b, c;

            a = b = c = (uint)(0xdeadbeef + data.Length + seed1);
            c += seed2;

            int index = 0, size = data.Length;
            while (size > 12)
            {
                a += BitConverter.ToUInt32(data, index);
                b += BitConverter.ToUInt32(data, index + 4);
                c += BitConverter.ToUInt32(data, index + 8);

                a -= c;
                a ^= (c << 4) | (c >> 28);
                c += b;

                b -= a;
                b ^= (a << 6) | (a >> 26);
                a += c;

                c -= b;
                c ^= (b << 8) | (b >> 24);
                b += a;

                a -= c;
                a ^= (c << 16) | (c >> 16);
                c += b;

                b -= a;
                b ^= (a << 19) | (a >> 13);
                a += c;

                c -= b;
                c ^= (b << 4) | (b >> 28);
                b += a;

                index += 12;
                size -= 12;
            }

            switch (size)
            {
                case 12:
                    a += BitConverter.ToUInt32(data, index);
                    b += BitConverter.ToUInt32(data, index + 4);
                    c += BitConverter.ToUInt32(data, index + 8);
                    break;

                case 11:
                    c += ((uint)data[index + 10]) << 16;
                    goto case 10;
                case 10:
                    c += ((uint)data[index + 9]) << 8;
                    goto case 9;
                case 9:
                    c += (uint)data[index + 8];
                    goto case 8;
                case 8:
                    b += BitConverter.ToUInt32(data, index + 4);
                    a += BitConverter.ToUInt32(data, index);
                    break;

                case 7:
                    b += ((uint)data[index + 6]) << 16;
                    goto case 6;
                case 6:
                    b += ((uint)data[index + 5]) << 8;
                    goto case 5;
                case 5:
                    b += ((uint)data[index + 4]);
                    goto case 4;
                case 4:
                    a += BitConverter.ToUInt32(data, index);
                    break;

                case 3:
                    a += ((uint)data[index + 2]) << 16;
                    goto case 2;
                case 2:
                    a += ((uint)data[index + 1]) << 8;
                    goto case 1;
                case 1:
                    a += (uint)data[index];
                    break;

                case 0:
                    hash1 = c;
                    hash2 = b;
                    return;
            }

            c ^= b;
            c -= (b << 14) | (b >> 18);

            a ^= c;
            a -= (c << 11) | (c >> 21);

            b ^= a;
            b -= (a << 25) | (a >> 7);

            c ^= b;
            c -= (b << 16) | (b >> 16);

            a ^= c;
            a -= (c << 4) | (c >> 28);

            b ^= a;
            b -= (a << 14) | (a >> 18);

            c ^= b;
            c -= (b << 24) | (b >> 8);

            hash1 = c;
            hash2 = b;
        }
    }
}
