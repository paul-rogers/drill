package org.apache.drill.exec.expr;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.sun.codemodel.JFormatter;

public class DebugStringBuilder {
  
  private StringWriter strWriter;
  public PrintWriter writer;
  public JFormatter fmt;

  public DebugStringBuilder( Object obj ) {
    strWriter = new StringWriter( );
    writer = new PrintWriter( strWriter );
    writer.print( "[" );
    writer.print( obj.getClass().getSimpleName() );
    writer.print( ": " );
    fmt = new JFormatter( writer );
  }
  
  public DebugStringBuilder append( String s ) {
    writer.print( s );
    return this;
  }
  
  @Override
  public String toString( ) {
    writer.print( "]" );
    writer.flush();
    return strWriter.toString();
  }

}
