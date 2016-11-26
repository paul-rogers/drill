/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import com.google.common.io.Closeables;

public class FileMatcher {

  public interface Filter {
    String filter( String line );
  }

  public static interface Source {
    Reader reader( ) throws IOException;
  }

  public static class FileSource implements Source {
    File file;

    public FileSource( File file ) {
      this.file = file;
    }

    @Override
    public Reader reader( ) throws IOException {
      return new FileReader( file );
    }
  }

  public static class StringSource implements Source {

    String string;

    public StringSource( String string ) {
      this.string = string;
    }

    @Override
    public Reader reader( ) {
      return new StringReader( string );
    }
  }

  public static class ReaderSource implements Source {

    Reader reader;

    public ReaderSource( Reader reader ) {
      this.reader = reader;
    }

    @Override
    public Reader reader( ) {
      return reader;
    }
  }

  public static class ResourceSource implements Source {

    String resource;

    public ResourceSource( String resource ) {
      this.resource = resource;
    }

    @Override
    public Reader reader( ) {
      try {
        InputStream stream = getClass( ).getResourceAsStream( resource );
        if ( stream == null ) {
          throw new IllegalStateException( "Resource not found: " + resource );
        }
        return new InputStreamReader( stream, "UTF-8" );
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException( e );
      }
    }
  }

  public static class Builder {
    private boolean ignoreWhitespace = true;
    private Filter filter;
    private Source expected;
    private Source actual;
    private boolean capture;
    public boolean allowComments = true;

    public Builder expectedFile( File file ) {
      expected = new FileSource( file );
      return this;
    }

    public Builder expectedString( String text ) {
      expected = new StringSource( text );
      return this;
    }

    public Builder expectedReader( Reader in ) {
      expected = new ReaderSource( in );
      return this;
    }

    public Builder expectedResource( String resource ) {
      expected = new ResourceSource( resource );
      return this;
    }

    public Builder actualFile( File file ) {
      actual = new FileSource( file );
      return this;
    }

    public Builder actualString( String text ) {
      actual = new StringSource( text );
      return this;
    }

    public Builder actualReader( Reader in ) {
      actual = new ReaderSource( in );
      return this;
    }

    public Builder actualResource( String resource ) {
      actual = new ResourceSource( resource );
      return this;
    }

    public Builder preserveWhitespace( ) {
      ignoreWhitespace = false;
      return this;
    }

    public Builder disallowComments( ) {
      allowComments = false;
      return this;
    }

    public Builder withFilter( Filter filter ) {
      this.filter = filter;
      return this;
    }

    public Builder capture( ) {
      this.capture = true;
      return this;
    }

    public boolean matches( ) throws IOException {
      FileMatcher matcher = new FileMatcher( this );
      if ( capture ) {
        matcher.capture( );
        return true;
      } else {
        return matcher.match( );
      }
    }
  }

  private Builder builder;
  private LineNumberReader expected;
  private LineNumberReader actual;

  private FileMatcher( Builder builder ) {
    this.builder = builder;
  }

  public void capture( ) throws IOException {
    try (LineNumberReader actual = new LineNumberReader( builder.actual.reader() )) {
      System.out.println( "----- Captured Results -----" );
      String line;
      while ((line = actual.readLine()) != null ) {
        if ( builder.filter != null ) {
          line = builder.filter.filter( line );
          if ( line == null ) {
            continue; }
        }
        System.out.println( line );
      }
      System.out.println( "----- End Results -----" );
    }
  }

  public boolean match( ) throws IOException {
    try {
      open( );
      return compare( );
    }
    finally {
      close( );
    }
  }

  private void open() throws IOException {
    expected = new LineNumberReader( builder.expected.reader() );
    actual = new LineNumberReader( builder.actual.reader() );
  }

  private boolean compare() throws IOException {
    for ( ; ; ) {
      String expectedLine;
      for ( ; ; ) {
        expectedLine = expected.readLine();
        if ( expectedLine == null ) {
          break; }
        if ( builder.ignoreWhitespace ) {
          expectedLine = expectedLine.trim();
          if ( expectedLine.isEmpty() ) {
            continue; }
        }
        if ( builder.allowComments && expectedLine.charAt(0) == '#' ) {
          continue; }
        break;
      }
      String actualLine;
      for ( ; ; ) {
        actualLine = actual.readLine();
        if ( actualLine == null ) {
          break; }
        if ( builder.filter != null ) {
          actualLine = builder.filter.filter( actualLine );
          if ( actualLine == null ) {
            continue; }
        }
        if ( builder.ignoreWhitespace ) {
          actualLine = actualLine.trim();
          if ( actualLine.isEmpty() ) {
            continue; }
        }
        break;
      }
      if ( expectedLine == null  &&  actualLine == null ) {
        return true; }
      if ( expectedLine == null ) {
        System.err.println( "Missing lines at line " + expected.getLineNumber() );
        return false;
      }
      if ( actualLine == null ) {
        System.err.println( "Unexpected lines at line " + expected.getLineNumber() );
        return false;
      }
      if ( ! actualLine.equals( expectedLine ) ) {
        System.err.println( "Lines differ. Expected: " + expected.getLineNumber() +
                            ", Actual: " + actual.getLineNumber() );
        return false;
      }
    }
  }

  private void close() {
    Closeables.closeQuietly( expected );
    Closeables.closeQuietly( actual );
  }
}
