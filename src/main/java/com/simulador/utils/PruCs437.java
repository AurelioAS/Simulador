package com.simulador.utils;

import java.nio.charset.Charset;
import org.bouncycastle.util.Arrays;

public class PruCs437 {

  private static final int N = 8;

  private static final int SPACE = 13;

  private static final Charset CS = Charset.forName("Cp437");

  public static void main(String[] args) {
    int nlines = 2*N+1;
    int linesize = 4*N+1;
    int size = nlines * linesize;
    byte[] arr = new byte[size+100];
    int[] TB1 = { 0xc9, 0xd1, 0xbb, 0xc7, 0xc5, 0xb6, 0xc8, 0xcf, 0xbc, 0xc4, 0xb3, 0xcd, 0xba, 0x20 };
    
    for (int i=0; i<size; i++)
      arr[i] = (byte) TB1[SPACE];
    
    for (int i=0; i<9; i++) {
      byte ch = (byte) TB1[i];
      int y = (i/3) % 3;
      int x = i % 3;
      if (y == 1 || x == 1) {
        for (int j=1; j<N; j++) {
          if (x == 1 && y == 1) {
            for (int k=1; k<N; k++) {
              int idx = j*2*linesize + k*4;
              arr[idx] = ch;
            }
          } else if (y == 1) {
            int idx = j*2*linesize + x*2*N;
            arr[idx] = ch;
          } else if (x == 1) {
            int idx = j*4 + y*linesize*N;
            arr[idx] = ch;
          }
        }
      } else {
        int idx = N*(linesize*y + 2*x);
        arr[idx] = ch;
      }
    }

    for (int i=0; i<2; i++) {
      int vertical = i % 2;
      int horizontal = 1 - vertical;
      for (int y=0; y<N+horizontal; y++) {
        for (int x=0; x<N+vertical; x++) {
          int dbl = horizontal==1 && (y==0 || y==N) || vertical==1 && (x==0 || x==N) ? 1 : 0;
          byte ch = (byte) TB1[9+2*dbl+vertical];
          int idx = 2*linesize*y + 4*x + horizontal + linesize*vertical;
          int nc = 1 + 2*horizontal;
          for (int k=0; k<nc; k++)
            arr[idx+k] = ch;
        }
      }
    }
    
    byte[][] lines = new byte[nlines][linesize];
    for (int i=0; i<nlines; i++)
      lines[i] = Arrays.copyOfRange(arr, linesize*i, linesize*(i+1));
    
    for (byte[] line : lines) {
      System.out.println(new String(line, CS));
    }
  }
  
}
