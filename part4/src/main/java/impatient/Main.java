/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package impatient.part4;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;


public class
  Main
  {
  public static void
  main( String[] args ) throws Exception
    {

    String docPath = args[ 0 ];
    String wcPath = args[ 1 ];
    String stopPath1 = args[ 2 ];

    // to test multiple sources with identical file names but some differing
    // dir on the path: args[ 2 ] is /data1/en.stop args[ 3 ] is /data2/en.stop
    //
    String stopPath2 = args[ 3 ];

    Properties properties = new Properties();

    ClassLoader cl = Main.class.getClassLoader();
    URL prop4Url = cl.getResource("part4.properties");
    System.out.println("Found properties: " + prop4Url);
    InputStream propertiesAsStream = prop4Url.openStream();
    properties.load(propertiesAsStream);
    propertiesAsStream.close();
    AppProps.setApplicationJarClass( properties, Main.class );
    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create source and sink taps
    Tap docTap = new Hfs( new TextDelimited( true, "\t" ), docPath );
    Tap wcTap = new Hfs( new TextDelimited( true, "\t" ), wcPath );

    Fields stop1 = new Fields( "stop1" );
    Tap stopTap1 = new Hfs( new TextDelimited( stop1, true, "\t" ), stopPath1 );

    Fields stop2 = new Fields( "stop2" );
    Tap stopTap2 = new Hfs( new TextDelimited( stop2, true, "\t" ), stopPath2 );

    // specify a regex operation to split the "document" text lines into a token stream
    Fields token = new Fields( "token" );
    Fields text = new Fields( "text" );
    RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
    Fields fieldSelector = new Fields( "doc_id", "token" );
    Pipe docPipe = new Each( "token", text, splitter, fieldSelector );

    // define "ScrubFunction" to clean up the token stream
    Fields scrubArguments = new Fields( "doc_id", "token" );
    docPipe = new Each( docPipe, scrubArguments, new ScrubFunction( scrubArguments ), Fields.RESULTS );

    // perform a left join to remove stop words, discarding the rows
    // which joined with stop words, i.e., were non-null after left join
    Pipe stopPipe1 = new Pipe( "stop1" );
    Pipe stopPipe2 = new Pipe( "stop2" );
    Pipe tokenPipe = docPipe;

    tokenPipe = new Each(
        Boolean.valueOf( ( String )properties.get( "stop1.hashjoin" ) )
            ? new HashJoin( tokenPipe, token, stopPipe1, stop1, new LeftJoin() )
            : new CoGroup( tokenPipe, token, stopPipe1, stop1, new LeftJoin() ),
        stop1, new RegexFilter( "^$" ) );

    // test intermediate input into HashJoin, gby to force an identity MR job
    if( Boolean.valueOf( ( String )properties.get( "stop2.groupby" ) ) )
      stopPipe2 = new GroupBy( stopPipe2, stop2 );

    HashJoin hashJoin2 = new HashJoin(tokenPipe, token, stopPipe2, stop2, new LeftJoin());


    tokenPipe = new Each( hashJoin2, stop2, new RegexFilter( "^$" ) );

    // determine the word counts
    Pipe wcPipe = new Pipe( "wc", tokenPipe );
    wcPipe = new Retain( wcPipe, token );
    wcPipe = new GroupBy( wcPipe, token );
    wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
     .setName( "wc" )
     .addSource( docPipe, docTap )
     .addSource( stopPipe1, stopTap1 )
     .addSource( stopPipe2, stopTap2 )
     .addTailSink( wcPipe, wcTap );

    // write a DOT file and run the flow
    Flow wcFlow = flowConnector.connect( flowDef );
    wcFlow.writeDOT( "dot/wc.dot" );
    wcFlow.complete();
    }
  }
