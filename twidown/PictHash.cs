﻿using System;
using System.Runtime.InteropServices;
using System.IO;
using System.Drawing;
using System.Drawing.Imaging;
using System.Linq;
using System.Numerics;

//using System.Windows.Media;
//using System.Windows.Media.Imaging;

namespace twidown
{
    static class PictHash
    {
        //static readonly float[,] dct32; //[1,1]-[8,8]だけ
        static readonly float[] cos32;
        static readonly int VectorCount;    //Vector<float>の要素数
        static readonly Vector<float>[] cos32h1_8; //横方向にVectorCountずつ取った奴
        static PictHash()
        {
            //DCT係数テーブル 対称行列だからxyは気にするな
            //[1,1]-[8,8]だけを切り抜いて格納
            /* ということは使う部分は全部 1/64 なので完全に用なし
            dct32 = new float[8, 8];
            for (int u = 1; u < 9; u++)
                for (int v = 1; v < 9; v++)
                {
                    if (u * v == 0) { dct32[u - 1, v - 1] = (float)Math.Sqrt(0.5) / 64; }
                    else { dct32[u - 1, v - 1] = 1F / 64F; }
                }
            //dct32[0, 0] = 0.5F / 64F; 原点をずらしたから用なし
            */
            //Cosテーブル[行 << 5 | 列]
            //1~8行を切り抜いておく
            cos32 = new float[256];
            for (int y = 0; y < 8; y++)
                for (int x = 0; x < 32; x++)
                {
                    cos32[y << 5 | x] = (float)(Math.Cos((2 * x + 1) * (y + 1) * Math.PI / 64));
                }

            VectorCount = Math.Min(32, Vector<float>.Count);

            //こっちも1~8行だけ使う
            cos32h1_8 = new Vector<float>[256 / VectorCount];
            float[] costmph = new float[VectorCount];
            for (int y = 0; y < 8; y++)
            {
                int n = 0;
                for (int x = 0; x < 32; x++)
                {
                    costmph[n] = cos32[y << 5 | x];
                    n++;
                    if (n == VectorCount)
                    {
                        cos32h1_8[(y << 5 | x) / VectorCount] = new Vector<float>(costmph);
                        n = 0;
                    }
                }
            }
        }

        public static long? dcthash(string imgPath)
        {
            try
            {
                using (FileStream imgStream = new FileStream(imgPath, FileMode.Open))
                {
                    return dcthash(imgStream);
                }
            }
            catch { return null; }
        }
        
        //pHashっぽいDCT Hashの自前実装
        public static long? dcthash(Stream imgStream, bool crop = false)
        {
            Vector<float>[] hashbuf = monoimage(imgStream, 32, crop); //モノクロ縮小画像
            if (hashbuf == null) { return null; }  //正常な場合は0にはならない

            //DCTやる phashで必要な成分だけ求める
            //http://www.ice.gunma-ct.ac.jp/~tsurumi/courses/ImagePro/DCT/2D_DCT.htm
            int xCount = 32 / VectorCount;
            float[] dctbuf = new float[64];
            for (int u = 0; u < 8; u++)
                for (int v = 0; v < 8; v++)
                {
                    /*
                    float s = 0;
                    for (int y = 0; y < 32; y++)
                        for (int x = 0; x < 32; x++)
                        {
                            //s += cos32[u + 1, y] * hashbuf[((y << 5) + x) / VectorCount][x % VectorCount] * cos32[v + 1, x];
                            s += cos32[u + 1, y] * hashbuf[((y << 5) + x) >> 2][x & 3] * cos32[v + 1, x];
                        }
                    dctbuf[u * 8 + v] = s * dct32[u, v];
                    */
                    float sum = 0;
                    for (int y = 0; y < 32; y++)
                    {
                        Vector<float> tosumvec = hashbuf[y * xCount] * cos32h1_8[v * xCount];
                        for (int x = 1; x < xCount; x++)
                        {
                            tosumvec += hashbuf[y * xCount + x] * cos32h1_8[v * xCount + x];
                        }
                        float tosum = tosumvec[0];
                        for (int i = 1; i < VectorCount; i++)
                        {
                            tosum += tosumvec[i];
                        }
                        sum += tosum * cos32[u << 5 | y];
                    }
                    dctbuf[(u << 3) | v] = sum; //dct32[u, v]をかけていないので実際の64倍の値
                }

            long ret = 0;
            float ave = dctbuf.Average();
            for (int i = 0; i < 64; i++)
            {
                if (dctbuf[i] < ave) { ret |= 1L << i; }
                //ret |= (Math.Sign(dctbuf[i]) & 0x80000000) << 32 >> (63 - i); 
            }
            return ret;
        }

        //正方形、モノクロの画像に対応するバイト列を返す
        static Vector<float>[] monoimage(Stream imgStream, int size, bool crop = false)
        {
            try
            {
                using (Image img = Image.FromStream(imgStream)) 
                using (Bitmap miniimage = new Bitmap(size, size))
                using (Graphics g = Graphics.FromImage(miniimage))
                {
                    g.InterpolationMode = System.Drawing.Drawing2D.InterpolationMode.HighQualityBilinear;
                    g.PixelOffsetMode = System.Drawing.Drawing2D.PixelOffsetMode.Half;
                    if (crop)
                    {
                        Rectangle SizeRect = new Rectangle(0, 0, size, size);
                        Rectangle CropRect;
                        if (img.Width > img.Height) { CropRect = new Rectangle((img.Width - img.Height) / 2, 0, img.Height, img.Height); }
                        else { CropRect = new Rectangle(0, (img.Height - img.Width) / 2, img.Width, img.Width); }
                        g.DrawImage(img, SizeRect, CropRect, GraphicsUnit.Pixel);
                    }
                    else { g.DrawImage(img, 0, 0, size, size); }
    
                    //バイト配列に取り出す
                    //http://www.84kure.com/blog/2014/07/13/c-%E3%83%93%E3%83%83%E3%83%88%E3%83%9E%E3%83%83%E3%83%97%E3%81%AB%E3%83%94%E3%82%AF%E3%82%BB%E3%83%AB%E5%8D%98%E4%BD%8D%E3%81%A7%E9%AB%98%E9%80%9F%E3%81%AB%E3%82%A2%E3%82%AF%E3%82%BB%E3%82%B9/
                    BitmapData imgdata = miniimage.LockBits(new Rectangle(0, 0, size, size), ImageLockMode.ReadOnly, System.Drawing.Imaging.PixelFormat.Format32bppArgb);
                    byte[] imgbuf = new byte[size * size * 4];
                    Marshal.Copy(imgdata.Scan0, imgbuf, 0, imgbuf.Length);
                    //モノクロに変換
                    float[] hashbuf = new float[VectorCount]; //モノクロにしたやつを入れる
                    Vector<float>[] ret = new Vector<float>[size * size / VectorCount];  //VectorCount個ごとに並べたやつ
                    int i = 0;
                    for (int y = 0; y < size; y++)
                        for (int x = 0; x < size; x += VectorCount)
                        {
                            for (int n = 0; n < VectorCount; n++)
                            {
                                //RとBの係数が逆になっているが今更どうしようもない
                                hashbuf[n] = (0.299F * imgbuf[y * size + x + n << 2] + 0.587F * imgbuf[(y * size + x + n << 2) + 1] + 0.114F * imgbuf[(y * size + x + n << 2) + 2]);
                            }
                            ret[i] = new Vector<float>(hashbuf);
                            i++;
                        }
                    /*
                    for (int i = 0; i < imgbuf.Length; i += 4)
                    {
                        hashbuf[(i >> 2)] = (0.299F * imgbuf[i] + 0.587F * imgbuf[i + 1] + 0.114F * imgbuf[i + 2]);
                    }
                    */
                    return ret;
                }
            }
            catch
            {
                return null;
            }
        }
        
        /*
        static Vector<float>[] monoimagewpf(Stream imgStream, int size, bool crop = false)
        {
            //try
            {
                sww.Start();

                BitmapImage img = new BitmapImage();
                img.BeginInit();
                img.CacheOption = BitmapCacheOption.OnLoad;
                img.StreamSource = imgStream;
                img.EndInit();
                img.Freeze();
                
                TransformedBitmap miniimage = new TransformedBitmap();
                miniimage.BeginInit();
                if (img.Format != PixelFormats.Bgr32 && img.Format != PixelFormats.Bgra32)
                {
                    FormatConvertedBitmap imgrgb32 = new FormatConvertedBitmap(img, PixelFormats.Bgr32, null, 0);
                    miniimage.Source = imgrgb32;
                }
                else
                {
                    miniimage.Source = img;
                }
                miniimage.Transform = new ScaleTransform((double)size / (double)img.PixelWidth, (double)size / (double)img.PixelHeight);
                RenderOptions.SetBitmapScalingMode(miniimage, BitmapScalingMode.NearestNeighbor);
                miniimage.EndInit();
                miniimage.Freeze();

                //バイト配列に取り出す
                int nStride = (miniimage.PixelWidth * miniimage.Format.BitsPerPixel + 7) >> 3;
                byte[] imgbuf = new byte[miniimage.PixelHeight * nStride];
                miniimage.CopyPixels(imgbuf, nStride, 0);

                //モノクロに変換
                float[] hashbuf = new float[VectorCount]; //モノクロにしたやつを入れる
                Vector<float>[] ret = new Vector<float>[size * size / VectorCount];  //VectorCount個ごとに並べたやつ
                int i = 0;
                for (int y = 0; y < size; y++)
                    for (int x = 0; x < size; x += VectorCount)
                    {
                        for (int n = 0; n < VectorCount; n++)
                        {
                            hashbuf[n] = (0.299F * imgbuf[y * size + x + n << 2] + 0.587F * imgbuf[(y * size + x + n << 2) + 1] + 0.114F * imgbuf[(y * size + x + n << 2) + 2]);    //RとBの係数が逆になっているが今更どうしようもない
                        }
                        ret[i] = new Vector<float>(hashbuf);
                        i++;
                    }
                sww.Stop();
                return ret;
            }
            //catch
            {
                return null;
            }
        }
*/
    }
}
