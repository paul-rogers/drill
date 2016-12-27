package org.apache.drill.exec.physical.impl.xsort;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

public class SortTest {

  public static final int SIZE = 10_000_000;
  
  public static void main(String[] args) {
    for ( int i = 0;  i < 2;  i++ ) {
      testIntArray( );
      testIntegerArray( );
      testIntegerArrayComparator( );
      testIntegerHadoopQSInteger( );
      testIntegerHadoopQSInt( );
      testIntegerHadoopQSBuf( );
      testIntegerHadoopQSBufSV( );
    }
  }

  private static void testIntegerHadoopQSInteger() {
    Integer data[] = new Integer[SIZE];
    Random rand = new Random();
    for ( int i = 0; i < data.length; i++ ) {
      data[i] = rand.nextInt();
    }
    QuickSort qs = new QuickSort();

    long start = System.currentTimeMillis();
    qs.sort(new IndexedSortable( ) {

      @Override
      public int compare(int i, int j) {
        return Integer.compare(data[i], data[j]);
      }

      @Override
      public void swap(int i, int j) {
        Integer temp = data[i];
        data[i] = data[j];
        data[j] = temp;
      }

    }, 0, data.length);
    System.out.println( "Hadoop QS time (Integer): " + (System.currentTimeMillis() - start));
  }

  private static void testIntegerHadoopQSInt() {
    int data[] = new int[SIZE];
    Random rand = new Random();
    for ( int i = 0; i < data.length; i++ ) {
      data[i] = rand.nextInt();
    }
    QuickSort qs = new QuickSort();

    long start = System.currentTimeMillis();
    qs.sort(new IndexedSortable( ) {

      @Override
      public int compare(int i, int j) {
        return Integer.compare(data[i], data[j]);
      }

      @Override
      public void swap(int i, int j) {
        int temp = data[i];
        data[i] = data[j];
        data[j] = temp;
      }

    }, 0, data.length);
    System.out.println( "Hadoop QS time (int): " + (System.currentTimeMillis() - start));
  }

  private static void testIntegerHadoopQSBuf() {
    int n = SIZE;
    ByteBuffer buf = ByteBuffer.allocate(n * 4);
    Random rand = new Random();
    for ( int i = 0; i < n; i++ ) {
      buf.putInt(i*4, rand.nextInt() );
    }
    QuickSort qs = new QuickSort();

    long start = System.currentTimeMillis();
    qs.sort(new IndexedSortable( ) {

      @Override
      public int compare(int i, int j) {
        return Integer.compare(buf.getInt(i*4), buf.getInt(j*4));
      }

      @Override
      public void swap(int i, int j) {
        int temp = buf.getInt(i*4);
        buf.putInt(i*4, buf.getInt(j*4));
        buf.putInt(j*4, temp);
      }

    }, 0, n);
    System.out.println( "Hadoop QS time (ByteBuffer): " + (System.currentTimeMillis() - start));
  }

  private static void testIntegerHadoopQSBufSV() {
    int n = SIZE;
    ByteBuffer buf = ByteBuffer.allocate(n * 4);
    ByteBuffer sv = ByteBuffer.allocate(n * 4);
    Random rand = new Random();
    for ( int i = 0; i < n; i++ ) {
      buf.putInt(i*4, rand.nextInt() );
      sv.putInt(i*4, i);
    }
    QuickSort qs = new QuickSort();

    long start = System.currentTimeMillis();
    qs.sort(new IndexedSortable( ) {

      @Override
      public int compare(int i, int j) {
        int left = sv.getInt(i*4);
        int right = sv.getInt(j*4);
        return Integer.compare(buf.getInt(left*4), buf.getInt(right*4));
      }

      @Override
      public void swap(int i, int j) {
        int temp = sv.getInt(i*4);
        sv.putInt(i*4, sv.getInt(j*4));
        sv.putInt(j*4, temp);
      }

    }, 0, n);
    System.out.println( "Hadoop QS time (ByteBuffer, SV): " + (System.currentTimeMillis() - start));
  }

  private static void testIntegerArrayComparator() {
    Integer data[] = new Integer[SIZE];
    Random rand = new Random();
    for ( int i = 0; i < data.length; i++ ) {
      data[i] = rand.nextInt();
    }
    long start = System.currentTimeMillis();
    Arrays.sort(data, new Comparator<Integer>( ) {

      @Override
      public int compare(Integer o1, Integer o2) {
        return Integer.compare(o1, o2);
      }

    });
    System.out.println( "TimSort.sort(Integer, c) time: " + (System.currentTimeMillis() - start));
  }

  private static void testIntegerArray() {
    Integer data[] = new Integer[SIZE];
    Random rand = new Random();
    for ( int i = 0; i < data.length; i++ ) {
      data[i] = rand.nextInt();
    }
    long start = System.currentTimeMillis();
    Arrays.sort(data);
    System.out.println( "Arrays.sort(Integer) time: " + (System.currentTimeMillis() - start));
  }

  private static void testIntArray( ) {
    int data[] = new int[SIZE];
    Random rand = new Random();
    for ( int i = 0; i < data.length; i++ ) {
      data[i] = rand.nextInt();
    }
    long start = System.currentTimeMillis();
    Arrays.sort(data);
    System.out.println( "Arrays.sort(int) time: " + (System.currentTimeMillis() - start));
  }

}
