// $ANTLR 3.4 /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g 2014-07-14 11:47:04
 
package eu.stratosphere.meteor; 

import java.io.File;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked"})
public class MeteorLexer extends Lexer {
    public static final int EOF=-1;
    public static final int T__36=36;
    public static final int T__37=37;
    public static final int T__38=38;
    public static final int T__39=39;
    public static final int T__40=40;
    public static final int T__41=41;
    public static final int T__42=42;
    public static final int T__43=43;
    public static final int T__44=44;
    public static final int T__45=45;
    public static final int T__46=46;
    public static final int T__47=47;
    public static final int T__48=48;
    public static final int T__49=49;
    public static final int T__50=50;
    public static final int T__51=51;
    public static final int T__52=52;
    public static final int T__53=53;
    public static final int T__54=54;
    public static final int T__55=55;
    public static final int T__56=56;
    public static final int T__57=57;
    public static final int T__58=58;
    public static final int T__59=59;
    public static final int T__60=60;
    public static final int T__61=61;
    public static final int T__62=62;
    public static final int T__63=63;
    public static final int T__64=64;
    public static final int T__65=65;
    public static final int T__66=66;
    public static final int T__67=67;
    public static final int T__68=68;
    public static final int T__69=69;
    public static final int T__70=70;
    public static final int AND=4;
    public static final int APOSTROPHE=5;
    public static final int COMMENT=6;
    public static final int DECIMAL=7;
    public static final int DIGIT=8;
    public static final int ELSE=9;
    public static final int ESC_SEQ=10;
    public static final int EXPONENT=11;
    public static final int EXPRESSION=12;
    public static final int FN=13;
    public static final int HEX_DIGIT=14;
    public static final int ID=15;
    public static final int IF=16;
    public static final int IN=17;
    public static final int INCLUDE=18;
    public static final int INTEGER=19;
    public static final int JAVAUDF=20;
    public static final int LOWER_LETTER=21;
    public static final int NOT=22;
    public static final int OCTAL_ESC=23;
    public static final int OPERATOR=24;
    public static final int OR=25;
    public static final int QUOTATION=26;
    public static final int SIGN=27;
    public static final int SLASH=28;
    public static final int STAR=29;
    public static final int STRING=30;
    public static final int UINT=31;
    public static final int UNICODE_ESC=32;
    public static final int UPPER_LETTER=33;
    public static final int VAR=34;
    public static final int WS=35;

        class SaveStruct {
          SaveStruct(CharStream input){
            this.input = input;
            this.marker = input.mark();
          }
          public CharStream input;
          public int marker;
         }
     
         Stack<SaveStruct> includes = new Stack<SaveStruct>();
     
        // We should override this method for handling EOF of included file
         public Token nextToken(){
           Token token = super.nextToken();
     
           if(token.getType() == Token.EOF && !includes.empty()){
            // We've got EOF and have non empty stack.
             SaveStruct ss = includes.pop();
             setCharStream(ss.input);
             input.rewind(ss.marker);
             //this should be used instead of super [like below] to handle exits from nested includes
             //it matters, when the 'include' token is the last in previous stream (using super, lexer 'crashes' returning EOF token)
             token = this.nextToken();
           }
     
          // Skip first token after switching on another input.
          // You need to use this rather than super as there may be nested include files
           if(((CommonToken)token).getStartIndex() < 0)
             token = this.nextToken();
     
           return token;
         }
     

    // delegates
    // delegators
    public Lexer[] getDelegates() {
        return new Lexer[] {};
    }

    public MeteorLexer() {} 
    public MeteorLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public MeteorLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);
    }
    public String getGrammarFileName() { return "/home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g"; }

    // $ANTLR start "T__36"
    public final void mT__36() throws RecognitionException {
        try {
            int _type = T__36;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:47:7: ( '!' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:47:9: '!'
            {
            match('!'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__36"

    // $ANTLR start "T__37"
    public final void mT__37() throws RecognitionException {
        try {
            int _type = T__37;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:48:7: ( '!=' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:48:9: '!='
            {
            match("!="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__37"

    // $ANTLR start "T__38"
    public final void mT__38() throws RecognitionException {
        try {
            int _type = T__38;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:49:7: ( '\"' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:49:9: '\"'
            {
            match('\"'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__38"

    // $ANTLR start "T__39"
    public final void mT__39() throws RecognitionException {
        try {
            int _type = T__39;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:50:7: ( '&&' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:50:9: '&&'
            {
            match("&&"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__39"

    // $ANTLR start "T__40"
    public final void mT__40() throws RecognitionException {
        try {
            int _type = T__40;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:51:7: ( '&' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:51:9: '&'
            {
            match('&'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__40"

    // $ANTLR start "T__41"
    public final void mT__41() throws RecognitionException {
        try {
            int _type = T__41;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:52:7: ( '(' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:52:9: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__41"

    // $ANTLR start "T__42"
    public final void mT__42() throws RecognitionException {
        try {
            int _type = T__42;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:53:7: ( ')' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:53:9: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__42"

    // $ANTLR start "T__43"
    public final void mT__43() throws RecognitionException {
        try {
            int _type = T__43;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:54:7: ( '+' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:54:9: '+'
            {
            match('+'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__43"

    // $ANTLR start "T__44"
    public final void mT__44() throws RecognitionException {
        try {
            int _type = T__44;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:55:7: ( '++' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:55:9: '++'
            {
            match("++"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__44"

    // $ANTLR start "T__45"
    public final void mT__45() throws RecognitionException {
        try {
            int _type = T__45;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:56:7: ( ',' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:56:9: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__45"

    // $ANTLR start "T__46"
    public final void mT__46() throws RecognitionException {
        try {
            int _type = T__46;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:57:7: ( '-' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:57:9: '-'
            {
            match('-'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__46"

    // $ANTLR start "T__47"
    public final void mT__47() throws RecognitionException {
        try {
            int _type = T__47;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:58:7: ( '--' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:58:9: '--'
            {
            match("--"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__47"

    // $ANTLR start "T__48"
    public final void mT__48() throws RecognitionException {
        try {
            int _type = T__48;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:59:7: ( '.' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:59:9: '.'
            {
            match('.'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__48"

    // $ANTLR start "T__49"
    public final void mT__49() throws RecognitionException {
        try {
            int _type = T__49;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:60:7: ( ':' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:60:9: ':'
            {
            match(':'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__49"

    // $ANTLR start "T__50"
    public final void mT__50() throws RecognitionException {
        try {
            int _type = T__50;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:61:7: ( ';' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:61:9: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__50"

    // $ANTLR start "T__51"
    public final void mT__51() throws RecognitionException {
        try {
            int _type = T__51;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:62:7: ( '<' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:62:9: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__51"

    // $ANTLR start "T__52"
    public final void mT__52() throws RecognitionException {
        try {
            int _type = T__52;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:7: ( '<=' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:63:9: '<='
            {
            match("<="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__52"

    // $ANTLR start "T__53"
    public final void mT__53() throws RecognitionException {
        try {
            int _type = T__53;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:64:7: ( '=' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:64:9: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__53"

    // $ANTLR start "T__54"
    public final void mT__54() throws RecognitionException {
        try {
            int _type = T__54;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:65:7: ( '==' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:65:9: '=='
            {
            match("=="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__54"

    // $ANTLR start "T__55"
    public final void mT__55() throws RecognitionException {
        try {
            int _type = T__55;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:66:7: ( '>' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:66:9: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__55"

    // $ANTLR start "T__56"
    public final void mT__56() throws RecognitionException {
        try {
            int _type = T__56;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:67:7: ( '>=' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:67:9: '>='
            {
            match(">="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__56"

    // $ANTLR start "T__57"
    public final void mT__57() throws RecognitionException {
        try {
            int _type = T__57;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:68:7: ( '?' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:68:9: '?'
            {
            match('?'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__57"

    // $ANTLR start "T__58"
    public final void mT__58() throws RecognitionException {
        try {
            int _type = T__58;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:69:7: ( '?.' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:69:9: '?.'
            {
            match("?."); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__58"

    // $ANTLR start "T__59"
    public final void mT__59() throws RecognitionException {
        try {
            int _type = T__59;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:70:7: ( '[' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:70:9: '['
            {
            match('['); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__59"

    // $ANTLR start "T__60"
    public final void mT__60() throws RecognitionException {
        try {
            int _type = T__60;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:71:7: ( ']' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:71:9: ']'
            {
            match(']'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__60"

    // $ANTLR start "T__61"
    public final void mT__61() throws RecognitionException {
        try {
            int _type = T__61;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:72:7: ( 'false' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:72:9: 'false'
            {
            match("false"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__61"

    // $ANTLR start "T__62"
    public final void mT__62() throws RecognitionException {
        try {
            int _type = T__62;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:73:7: ( 'null' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:73:9: 'null'
            {
            match("null"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__62"

    // $ANTLR start "T__63"
    public final void mT__63() throws RecognitionException {
        try {
            int _type = T__63;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:74:7: ( 'read' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:74:9: 'read'
            {
            match("read"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__63"

    // $ANTLR start "T__64"
    public final void mT__64() throws RecognitionException {
        try {
            int _type = T__64;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:75:7: ( 'true' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:75:9: 'true'
            {
            match("true"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__64"

    // $ANTLR start "T__65"
    public final void mT__65() throws RecognitionException {
        try {
            int _type = T__65;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:76:7: ( 'using' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:76:9: 'using'
            {
            match("using"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__65"

    // $ANTLR start "T__66"
    public final void mT__66() throws RecognitionException {
        try {
            int _type = T__66;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:77:7: ( 'write' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:77:9: 'write'
            {
            match("write"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__66"

    // $ANTLR start "T__67"
    public final void mT__67() throws RecognitionException {
        try {
            int _type = T__67;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:78:7: ( '{' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:78:9: '{'
            {
            match('{'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__67"

    // $ANTLR start "T__68"
    public final void mT__68() throws RecognitionException {
        try {
            int _type = T__68;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:79:7: ( '||' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:79:9: '||'
            {
            match("||"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__68"

    // $ANTLR start "T__69"
    public final void mT__69() throws RecognitionException {
        try {
            int _type = T__69;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:80:7: ( '}' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:80:9: '}'
            {
            match('}'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__69"

    // $ANTLR start "T__70"
    public final void mT__70() throws RecognitionException {
        try {
            int _type = T__70;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:81:7: ( '~' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:81:9: '~'
            {
            match('~'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__70"

    // $ANTLR start "LOWER_LETTER"
    public final void mLOWER_LETTER() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:448:2: ( 'a' .. 'z' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            {
            if ( (input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LOWER_LETTER"

    // $ANTLR start "UPPER_LETTER"
    public final void mUPPER_LETTER() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:448:2: ( 'A' .. 'Z' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            {
            if ( (input.LA(1) >= 'A' && input.LA(1) <= 'Z') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "UPPER_LETTER"

    // $ANTLR start "DIGIT"
    public final void mDIGIT() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:451:2: ( '0' .. '9' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            {
            if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "DIGIT"

    // $ANTLR start "SIGN"
    public final void mSIGN() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:453:14: ( ( '+' | '-' ) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            {
            if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "SIGN"

    // $ANTLR start "JAVAUDF"
    public final void mJAVAUDF() throws RecognitionException {
        try {
            int _type = JAVAUDF;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:457:9: ( 'javaudf' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:457:11: 'javaudf'
            {
            match("javaudf"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "JAVAUDF"

    // $ANTLR start "OR"
    public final void mOR() throws RecognitionException {
        try {
            int _type = OR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:459:5: ( 'or' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:459:7: 'or'
            {
            match("or"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "OR"

    // $ANTLR start "AND"
    public final void mAND() throws RecognitionException {
        try {
            int _type = AND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:461:6: ( 'and' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:461:8: 'and'
            {
            match("and"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "AND"

    // $ANTLR start "IF"
    public final void mIF() throws RecognitionException {
        try {
            int _type = IF;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:463:5: ( 'if' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:463:7: 'if'
            {
            match("if"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "IF"

    // $ANTLR start "ELSE"
    public final void mELSE() throws RecognitionException {
        try {
            int _type = ELSE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:465:7: ( 'else' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:465:9: 'else'
            {
            match("else"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "ELSE"

    // $ANTLR start "NOT"
    public final void mNOT() throws RecognitionException {
        try {
            int _type = NOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:467:5: ( 'not' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:467:7: 'not'
            {
            match("not"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "NOT"

    // $ANTLR start "IN"
    public final void mIN() throws RecognitionException {
        try {
            int _type = IN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:469:5: ( 'in' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:469:7: 'in'
            {
            match("in"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "IN"

    // $ANTLR start "FN"
    public final void mFN() throws RecognitionException {
        try {
            int _type = FN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:471:5: ( 'fn' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:471:7: 'fn'
            {
            match("fn"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "FN"

    // $ANTLR start "ID"
    public final void mID() throws RecognitionException {
        try {
            int _type = ID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:473:4: ( ( LOWER_LETTER | UPPER_LETTER | '_' ) ( LOWER_LETTER | UPPER_LETTER | DIGIT | '_' )* )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:473:6: ( LOWER_LETTER | UPPER_LETTER | '_' ) ( LOWER_LETTER | UPPER_LETTER | DIGIT | '_' )*
            {
            if ( (input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:473:42: ( LOWER_LETTER | UPPER_LETTER | DIGIT | '_' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0 >= '0' && LA1_0 <= '9')||(LA1_0 >= 'A' && LA1_0 <= 'Z')||LA1_0=='_'||(LA1_0 >= 'a' && LA1_0 <= 'z')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "ID"

    // $ANTLR start "VAR"
    public final void mVAR() throws RecognitionException {
        try {
            int _type = VAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:475:5: ( '$' ID )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:475:7: '$' ID
            {
            match('$'); 

            mID(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "VAR"

    // $ANTLR start "STAR"
    public final void mSTAR() throws RecognitionException {
        try {
            int _type = STAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:477:6: ( '*' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:477:8: '*'
            {
            match('*'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "STAR"

    // $ANTLR start "COMMENT"
    public final void mCOMMENT() throws RecognitionException {
        try {
            int _type = COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:480:5: ( '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n' | '/*' ( options {greedy=false; } : . )* '*/' )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0=='/') ) {
                int LA5_1 = input.LA(2);

                if ( (LA5_1=='/') ) {
                    alt5=1;
                }
                else if ( (LA5_1=='*') ) {
                    alt5=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 5, 1, input);

                    throw nvae;

                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;

            }
            switch (alt5) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:480:9: '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n'
                    {
                    match("//"); 



                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:480:14: (~ ( '\\n' | '\\r' ) )*
                    loop2:
                    do {
                        int alt2=2;
                        int LA2_0 = input.LA(1);

                        if ( ((LA2_0 >= '\u0000' && LA2_0 <= '\t')||(LA2_0 >= '\u000B' && LA2_0 <= '\f')||(LA2_0 >= '\u000E' && LA2_0 <= '\uFFFF')) ) {
                            alt2=1;
                        }


                        switch (alt2) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    	    {
                    	    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '\t')||(input.LA(1) >= '\u000B' && input.LA(1) <= '\f')||(input.LA(1) >= '\u000E' && input.LA(1) <= '\uFFFF') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop2;
                        }
                    } while (true);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:480:28: ( '\\r' )?
                    int alt3=2;
                    int LA3_0 = input.LA(1);

                    if ( (LA3_0=='\r') ) {
                        alt3=1;
                    }
                    switch (alt3) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:480:28: '\\r'
                            {
                            match('\r'); 

                            }
                            break;

                    }


                    match('\n'); 

                    _channel=HIDDEN;

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:481:9: '/*' ( options {greedy=false; } : . )* '*/'
                    {
                    match("/*"); 



                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:481:14: ( options {greedy=false; } : . )*
                    loop4:
                    do {
                        int alt4=2;
                        int LA4_0 = input.LA(1);

                        if ( (LA4_0=='*') ) {
                            int LA4_1 = input.LA(2);

                            if ( (LA4_1=='/') ) {
                                alt4=2;
                            }
                            else if ( ((LA4_1 >= '\u0000' && LA4_1 <= '.')||(LA4_1 >= '0' && LA4_1 <= '\uFFFF')) ) {
                                alt4=1;
                            }


                        }
                        else if ( ((LA4_0 >= '\u0000' && LA4_0 <= ')')||(LA4_0 >= '+' && LA4_0 <= '\uFFFF')) ) {
                            alt4=1;
                        }


                        switch (alt4) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:481:42: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop4;
                        }
                    } while (true);


                    match("*/"); 



                    _channel=HIDDEN;

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "COMMENT"

    // $ANTLR start "SLASH"
    public final void mSLASH() throws RecognitionException {
        try {
            int _type = SLASH;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:485:5: ( '/' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:485:9: '/'
            {
            match('/'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "SLASH"

    // $ANTLR start "APOSTROPHE"
    public final void mAPOSTROPHE() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:489:3: ( '\\'' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:489:5: '\\''
            {
            match('\''); 

            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "APOSTROPHE"

    // $ANTLR start "QUOTATION"
    public final void mQUOTATION() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:492:3: ( '\\\"' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:492:5: '\\\"'
            {
            match('\"'); 

            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "QUOTATION"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:494:5: ( ( ' ' | '\\t' | '\\n' | '\\r' | ' ' )+ )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:494:7: ( ' ' | '\\t' | '\\n' | '\\r' | ' ' )+
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:494:7: ( ' ' | '\\t' | '\\n' | '\\r' | ' ' )+
            int cnt6=0;
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( ((LA6_0 >= '\t' && LA6_0 <= '\n')||LA6_0=='\r'||LA6_0==' '||LA6_0=='\u00A0') ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            	    {
            	    if ( (input.LA(1) >= '\t' && input.LA(1) <= '\n')||input.LA(1)=='\r'||input.LA(1)==' '||input.LA(1)=='\u00A0' ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt6 >= 1 ) break loop6;
                        EarlyExitException eee =
                            new EarlyExitException(6, input);
                        throw eee;
                }
                cnt6++;
            } while (true);


             skip(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "STRING"
    public final void mSTRING() throws RecognitionException {
        try {
            int _type = STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:497:2: ( ( QUOTATION ( options {greedy=false; } : . )* QUOTATION | APOSTROPHE ( options {greedy=false; } : . )* APOSTROPHE ) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:497:4: ( QUOTATION ( options {greedy=false; } : . )* QUOTATION | APOSTROPHE ( options {greedy=false; } : . )* APOSTROPHE )
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:497:4: ( QUOTATION ( options {greedy=false; } : . )* QUOTATION | APOSTROPHE ( options {greedy=false; } : . )* APOSTROPHE )
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0=='\"') ) {
                alt9=1;
            }
            else if ( (LA9_0=='\'') ) {
                alt9=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 9, 0, input);

                throw nvae;

            }
            switch (alt9) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:497:5: QUOTATION ( options {greedy=false; } : . )* QUOTATION
                    {
                    mQUOTATION(); 


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:497:15: ( options {greedy=false; } : . )*
                    loop7:
                    do {
                        int alt7=2;
                        int LA7_0 = input.LA(1);

                        if ( (LA7_0=='\"') ) {
                            alt7=2;
                        }
                        else if ( ((LA7_0 >= '\u0000' && LA7_0 <= '!')||(LA7_0 >= '#' && LA7_0 <= '\uFFFF')) ) {
                            alt7=1;
                        }


                        switch (alt7) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:497:42: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop7;
                        }
                    } while (true);


                    mQUOTATION(); 


                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:497:58: APOSTROPHE ( options {greedy=false; } : . )* APOSTROPHE
                    {
                    mAPOSTROPHE(); 


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:497:69: ( options {greedy=false; } : . )*
                    loop8:
                    do {
                        int alt8=2;
                        int LA8_0 = input.LA(1);

                        if ( (LA8_0=='\'') ) {
                            alt8=2;
                        }
                        else if ( ((LA8_0 >= '\u0000' && LA8_0 <= '&')||(LA8_0 >= '(' && LA8_0 <= '\uFFFF')) ) {
                            alt8=1;
                        }


                        switch (alt8) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:497:96: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop8;
                        }
                    } while (true);


                    mAPOSTROPHE(); 


                    }
                    break;

            }


             setText(getText().substring(1, getText().length()-1)); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "STRING"

    // $ANTLR start "ESC_SEQ"
    public final void mESC_SEQ() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:503:5: ( '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' ) | UNICODE_ESC | OCTAL_ESC )
            int alt10=3;
            int LA10_0 = input.LA(1);

            if ( (LA10_0=='\\') ) {
                switch ( input.LA(2) ) {
                case '\"':
                case '\'':
                case '\\':
                case 'b':
                case 'f':
                case 'n':
                case 'r':
                case 't':
                    {
                    alt10=1;
                    }
                    break;
                case 'u':
                    {
                    alt10=2;
                    }
                    break;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                    {
                    alt10=3;
                    }
                    break;
                default:
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 1, input);

                    throw nvae;

                }

            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;

            }
            switch (alt10) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:503:9: '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' )
                    {
                    match('\\'); 

                    if ( input.LA(1)=='\"'||input.LA(1)=='\''||input.LA(1)=='\\'||input.LA(1)=='b'||input.LA(1)=='f'||input.LA(1)=='n'||input.LA(1)=='r'||input.LA(1)=='t' ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:504:9: UNICODE_ESC
                    {
                    mUNICODE_ESC(); 


                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:505:9: OCTAL_ESC
                    {
                    mOCTAL_ESC(); 


                    }
                    break;

            }

        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "ESC_SEQ"

    // $ANTLR start "OCTAL_ESC"
    public final void mOCTAL_ESC() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:510:5: ( '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) )
            int alt11=3;
            int LA11_0 = input.LA(1);

            if ( (LA11_0=='\\') ) {
                int LA11_1 = input.LA(2);

                if ( ((LA11_1 >= '0' && LA11_1 <= '3')) ) {
                    int LA11_2 = input.LA(3);

                    if ( ((LA11_2 >= '0' && LA11_2 <= '7')) ) {
                        int LA11_4 = input.LA(4);

                        if ( ((LA11_4 >= '0' && LA11_4 <= '7')) ) {
                            alt11=1;
                        }
                        else {
                            alt11=2;
                        }
                    }
                    else {
                        alt11=3;
                    }
                }
                else if ( ((LA11_1 >= '4' && LA11_1 <= '7')) ) {
                    int LA11_3 = input.LA(3);

                    if ( ((LA11_3 >= '0' && LA11_3 <= '7')) ) {
                        alt11=2;
                    }
                    else {
                        alt11=3;
                    }
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 11, 1, input);

                    throw nvae;

                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;

            }
            switch (alt11) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:510:9: '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 

                    if ( (input.LA(1) >= '0' && input.LA(1) <= '3') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:511:9: '\\\\' ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 

                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:512:9: '\\\\' ( '0' .. '7' )
                    {
                    match('\\'); 

                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;

            }

        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "OCTAL_ESC"

    // $ANTLR start "UNICODE_ESC"
    public final void mUNICODE_ESC() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:516:13: ( '\\\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:516:17: '\\\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
            {
            match('\\'); 

            match('u'); 

            mHEX_DIGIT(); 


            mHEX_DIGIT(); 


            mHEX_DIGIT(); 


            mHEX_DIGIT(); 


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "UNICODE_ESC"

    // $ANTLR start "HEX_DIGIT"
    public final void mHEX_DIGIT() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:519:11: ( ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' ) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            {
            if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "HEX_DIGIT"

    // $ANTLR start "UINT"
    public final void mUINT() throws RecognitionException {
        try {
            int _type = UINT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:521:6: ( ( '0' .. '9' )+ )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:521:8: ( '0' .. '9' )+
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:521:8: ( '0' .. '9' )+
            int cnt12=0;
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( ((LA12_0 >= '0' && LA12_0 <= '9')) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt12 >= 1 ) break loop12;
                        EarlyExitException eee =
                            new EarlyExitException(12, input);
                        throw eee;
                }
                cnt12++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "UINT"

    // $ANTLR start "INTEGER"
    public final void mINTEGER() throws RecognitionException {
        try {
            int _type = INTEGER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:523:9: ( ( '+' | '-' )? UINT )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:523:11: ( '+' | '-' )? UINT
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:523:11: ( '+' | '-' )?
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( (LA13_0=='+'||LA13_0=='-') ) {
                alt13=1;
            }
            switch (alt13) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;

            }


            mUINT(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "INTEGER"

    // $ANTLR start "DECIMAL"
    public final void mDECIMAL() throws RecognitionException {
        try {
            int _type = DECIMAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:526:5: ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )? | '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '0' .. '9' )+ EXPONENT )
            int alt20=3;
            alt20 = dfa20.predict(input);
            switch (alt20) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:526:9: ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )?
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:526:9: ( '0' .. '9' )+
                    int cnt14=0;
                    loop14:
                    do {
                        int alt14=2;
                        int LA14_0 = input.LA(1);

                        if ( ((LA14_0 >= '0' && LA14_0 <= '9')) ) {
                            alt14=1;
                        }


                        switch (alt14) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt14 >= 1 ) break loop14;
                                EarlyExitException eee =
                                    new EarlyExitException(14, input);
                                throw eee;
                        }
                        cnt14++;
                    } while (true);


                    match('.'); 

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:526:25: ( '0' .. '9' )*
                    loop15:
                    do {
                        int alt15=2;
                        int LA15_0 = input.LA(1);

                        if ( ((LA15_0 >= '0' && LA15_0 <= '9')) ) {
                            alt15=1;
                        }


                        switch (alt15) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop15;
                        }
                    } while (true);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:526:37: ( EXPONENT )?
                    int alt16=2;
                    int LA16_0 = input.LA(1);

                    if ( (LA16_0=='E'||LA16_0=='e') ) {
                        alt16=1;
                    }
                    switch (alt16) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:526:37: EXPONENT
                            {
                            mEXPONENT(); 


                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:527:9: '.' ( '0' .. '9' )+ ( EXPONENT )?
                    {
                    match('.'); 

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:527:13: ( '0' .. '9' )+
                    int cnt17=0;
                    loop17:
                    do {
                        int alt17=2;
                        int LA17_0 = input.LA(1);

                        if ( ((LA17_0 >= '0' && LA17_0 <= '9')) ) {
                            alt17=1;
                        }


                        switch (alt17) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt17 >= 1 ) break loop17;
                                EarlyExitException eee =
                                    new EarlyExitException(17, input);
                                throw eee;
                        }
                        cnt17++;
                    } while (true);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:527:25: ( EXPONENT )?
                    int alt18=2;
                    int LA18_0 = input.LA(1);

                    if ( (LA18_0=='E'||LA18_0=='e') ) {
                        alt18=1;
                    }
                    switch (alt18) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:527:25: EXPONENT
                            {
                            mEXPONENT(); 


                            }
                            break;

                    }


                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:528:9: ( '0' .. '9' )+ EXPONENT
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:528:9: ( '0' .. '9' )+
                    int cnt19=0;
                    loop19:
                    do {
                        int alt19=2;
                        int LA19_0 = input.LA(1);

                        if ( ((LA19_0 >= '0' && LA19_0 <= '9')) ) {
                            alt19=1;
                        }


                        switch (alt19) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt19 >= 1 ) break loop19;
                                EarlyExitException eee =
                                    new EarlyExitException(19, input);
                                throw eee;
                        }
                        cnt19++;
                    } while (true);


                    mEXPONENT(); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "DECIMAL"

    // $ANTLR start "EXPONENT"
    public final void mEXPONENT() throws RecognitionException {
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:532:10: ( ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+ )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:532:12: ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:532:22: ( '+' | '-' )?
            int alt21=2;
            int LA21_0 = input.LA(1);

            if ( (LA21_0=='+'||LA21_0=='-') ) {
                alt21=1;
            }
            switch (alt21) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;

            }


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:532:33: ( '0' .. '9' )+
            int cnt22=0;
            loop22:
            do {
                int alt22=2;
                int LA22_0 = input.LA(1);

                if ( ((LA22_0 >= '0' && LA22_0 <= '9')) ) {
                    alt22=1;
                }


                switch (alt22) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt22 >= 1 ) break loop22;
                        EarlyExitException eee =
                            new EarlyExitException(22, input);
                        throw eee;
                }
                cnt22++;
            } while (true);


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "EXPONENT"

    // $ANTLR start "INCLUDE"
    public final void mINCLUDE() throws RecognitionException {
        try {
            int _type = INCLUDE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            CommonToken f=null;

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:535:6: ( 'include' ( WS )? f= STRING )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:535:8: 'include' ( WS )? f= STRING
            {
            match("include"); 



            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:535:18: ( WS )?
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( ((LA23_0 >= '\t' && LA23_0 <= '\n')||LA23_0=='\r'||LA23_0==' '||LA23_0=='\u00A0') ) {
                alt23=1;
            }
            switch (alt23) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:535:19: WS
                    {
                    mWS(); 


                    }
                    break;

            }


            int fStart966 = getCharIndex();
            int fStartLine966 = getLine();
            int fStartCharPos966 = getCharPositionInLine();
            mSTRING(); 
            f = new CommonToken(input, Token.INVALID_TOKEN_TYPE, Token.DEFAULT_CHANNEL, fStart966, getCharIndex()-1);
            f.setLine(fStartLine966);
            f.setCharPositionInLine(fStartCharPos966);



                  String name = f.getText();
                  name = name.substring(1, name.length() - 1);
                  File file = new File(name);
                  if(!file.exists() && !file.isAbsolute() && this.input instanceof ANTLRFileStream)
                    file = new File(new File(this.input.getSourceName()).getParent(), name);
                  try {
                    // save current lexer's state
                    SaveStruct ss = new SaveStruct(input);
                    includes.push(ss);

                    // switch on new input stream
                    setCharStream(new ANTLRFileStream(file.getAbsolutePath()));
                    reset();

                  } catch(Exception e) { throw new RuntimeException("Cannot open file " + name, e); }
                

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "INCLUDE"

    public void mTokens() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:8: ( T__36 | T__37 | T__38 | T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | T__45 | T__46 | T__47 | T__48 | T__49 | T__50 | T__51 | T__52 | T__53 | T__54 | T__55 | T__56 | T__57 | T__58 | T__59 | T__60 | T__61 | T__62 | T__63 | T__64 | T__65 | T__66 | T__67 | T__68 | T__69 | T__70 | JAVAUDF | OR | AND | IF | ELSE | NOT | IN | FN | ID | VAR | STAR | COMMENT | SLASH | WS | STRING | UINT | INTEGER | DECIMAL | INCLUDE )
        int alt24=54;
        alt24 = dfa24.predict(input);
        switch (alt24) {
            case 1 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:10: T__36
                {
                mT__36(); 


                }
                break;
            case 2 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:16: T__37
                {
                mT__37(); 


                }
                break;
            case 3 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:22: T__38
                {
                mT__38(); 


                }
                break;
            case 4 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:28: T__39
                {
                mT__39(); 


                }
                break;
            case 5 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:34: T__40
                {
                mT__40(); 


                }
                break;
            case 6 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:40: T__41
                {
                mT__41(); 


                }
                break;
            case 7 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:46: T__42
                {
                mT__42(); 


                }
                break;
            case 8 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:52: T__43
                {
                mT__43(); 


                }
                break;
            case 9 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:58: T__44
                {
                mT__44(); 


                }
                break;
            case 10 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:64: T__45
                {
                mT__45(); 


                }
                break;
            case 11 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:70: T__46
                {
                mT__46(); 


                }
                break;
            case 12 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:76: T__47
                {
                mT__47(); 


                }
                break;
            case 13 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:82: T__48
                {
                mT__48(); 


                }
                break;
            case 14 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:88: T__49
                {
                mT__49(); 


                }
                break;
            case 15 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:94: T__50
                {
                mT__50(); 


                }
                break;
            case 16 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:100: T__51
                {
                mT__51(); 


                }
                break;
            case 17 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:106: T__52
                {
                mT__52(); 


                }
                break;
            case 18 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:112: T__53
                {
                mT__53(); 


                }
                break;
            case 19 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:118: T__54
                {
                mT__54(); 


                }
                break;
            case 20 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:124: T__55
                {
                mT__55(); 


                }
                break;
            case 21 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:130: T__56
                {
                mT__56(); 


                }
                break;
            case 22 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:136: T__57
                {
                mT__57(); 


                }
                break;
            case 23 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:142: T__58
                {
                mT__58(); 


                }
                break;
            case 24 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:148: T__59
                {
                mT__59(); 


                }
                break;
            case 25 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:154: T__60
                {
                mT__60(); 


                }
                break;
            case 26 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:160: T__61
                {
                mT__61(); 


                }
                break;
            case 27 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:166: T__62
                {
                mT__62(); 


                }
                break;
            case 28 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:172: T__63
                {
                mT__63(); 


                }
                break;
            case 29 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:178: T__64
                {
                mT__64(); 


                }
                break;
            case 30 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:184: T__65
                {
                mT__65(); 


                }
                break;
            case 31 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:190: T__66
                {
                mT__66(); 


                }
                break;
            case 32 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:196: T__67
                {
                mT__67(); 


                }
                break;
            case 33 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:202: T__68
                {
                mT__68(); 


                }
                break;
            case 34 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:208: T__69
                {
                mT__69(); 


                }
                break;
            case 35 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:214: T__70
                {
                mT__70(); 


                }
                break;
            case 36 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:220: JAVAUDF
                {
                mJAVAUDF(); 


                }
                break;
            case 37 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:228: OR
                {
                mOR(); 


                }
                break;
            case 38 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:231: AND
                {
                mAND(); 


                }
                break;
            case 39 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:235: IF
                {
                mIF(); 


                }
                break;
            case 40 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:238: ELSE
                {
                mELSE(); 


                }
                break;
            case 41 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:243: NOT
                {
                mNOT(); 


                }
                break;
            case 42 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:247: IN
                {
                mIN(); 


                }
                break;
            case 43 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:250: FN
                {
                mFN(); 


                }
                break;
            case 44 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:253: ID
                {
                mID(); 


                }
                break;
            case 45 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:256: VAR
                {
                mVAR(); 


                }
                break;
            case 46 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:260: STAR
                {
                mSTAR(); 


                }
                break;
            case 47 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:265: COMMENT
                {
                mCOMMENT(); 


                }
                break;
            case 48 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:273: SLASH
                {
                mSLASH(); 


                }
                break;
            case 49 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:279: WS
                {
                mWS(); 


                }
                break;
            case 50 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:282: STRING
                {
                mSTRING(); 


                }
                break;
            case 51 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:289: UINT
                {
                mUINT(); 


                }
                break;
            case 52 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:294: INTEGER
                {
                mINTEGER(); 


                }
                break;
            case 53 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:302: DECIMAL
                {
                mDECIMAL(); 


                }
                break;
            case 54 :
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:310: INCLUDE
                {
                mINCLUDE(); 


                }
                break;

        }

    }


    protected DFA20 dfa20 = new DFA20(this);
    protected DFA24 dfa24 = new DFA24(this);
    static final String DFA20_eotS =
        "\5\uffff";
    static final String DFA20_eofS =
        "\5\uffff";
    static final String DFA20_minS =
        "\2\56\3\uffff";
    static final String DFA20_maxS =
        "\1\71\1\145\3\uffff";
    static final String DFA20_acceptS =
        "\2\uffff\1\2\1\1\1\3";
    static final String DFA20_specialS =
        "\5\uffff}>";
    static final String[] DFA20_transitionS = {
            "\1\2\1\uffff\12\1",
            "\1\3\1\uffff\12\1\13\uffff\1\4\37\uffff\1\4",
            "",
            "",
            ""
    };

    static final short[] DFA20_eot = DFA.unpackEncodedString(DFA20_eotS);
    static final short[] DFA20_eof = DFA.unpackEncodedString(DFA20_eofS);
    static final char[] DFA20_min = DFA.unpackEncodedStringToUnsignedChars(DFA20_minS);
    static final char[] DFA20_max = DFA.unpackEncodedStringToUnsignedChars(DFA20_maxS);
    static final short[] DFA20_accept = DFA.unpackEncodedString(DFA20_acceptS);
    static final short[] DFA20_special = DFA.unpackEncodedString(DFA20_specialS);
    static final short[][] DFA20_transition;

    static {
        int numStates = DFA20_transitionS.length;
        DFA20_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA20_transition[i] = DFA.unpackEncodedString(DFA20_transitionS[i]);
        }
    }

    class DFA20 extends DFA {

        public DFA20(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 20;
            this.eot = DFA20_eot;
            this.eof = DFA20_eof;
            this.min = DFA20_min;
            this.max = DFA20_max;
            this.accept = DFA20_accept;
            this.special = DFA20_special;
            this.transition = DFA20_transition;
        }
        public String getDescription() {
            return "525:1: DECIMAL : ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )? | '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '0' .. '9' )+ EXPONENT );";
        }
    }
    static final String DFA24_eotS =
        "\1\uffff\1\51\1\52\1\54\2\uffff\1\56\1\uffff\1\61\1\62\2\uffff\1"+
        "\65\1\67\1\71\1\73\2\uffff\6\41\4\uffff\5\41\3\uffff\1\113\2\uffff"+
        "\1\114\24\uffff\1\41\1\116\7\41\1\126\1\41\1\130\1\132\1\41\3\uffff"+
        "\1\41\1\uffff\1\41\1\136\5\41\1\uffff\1\144\1\uffff\1\41\1\uffff"+
        "\2\41\1\150\1\uffff\1\151\1\152\3\41\1\uffff\1\41\1\157\1\160\3"+
        "\uffff\1\161\1\162\2\41\4\uffff\2\41\1\167\1\41\2\uffff";
    static final String DFA24_eofS =
        "\171\uffff";
    static final String DFA24_minS =
        "\1\11\1\75\1\0\1\46\2\uffff\1\53\1\uffff\1\55\1\60\2\uffff\3\75"+
        "\1\56\2\uffff\1\141\1\157\1\145\1\162\1\163\1\162\4\uffff\1\141"+
        "\1\162\1\156\1\146\1\154\3\uffff\1\52\2\uffff\1\56\24\uffff\1\154"+
        "\1\60\1\154\1\164\1\141\1\165\2\151\1\166\1\60\1\144\2\60\1\163"+
        "\3\uffff\1\163\1\uffff\1\154\1\60\1\144\1\145\1\156\1\164\1\141"+
        "\1\uffff\1\60\1\uffff\1\154\1\uffff\2\145\1\60\1\uffff\2\60\1\147"+
        "\1\145\1\165\1\uffff\1\165\2\60\3\uffff\2\60\2\144\4\uffff\1\146"+
        "\1\145\1\60\1\11\2\uffff";
    static final String DFA24_maxS =
        "\1\u00a0\1\75\1\uffff\1\46\2\uffff\1\71\1\uffff\2\71\2\uffff\3\75"+
        "\1\56\2\uffff\1\156\1\165\1\145\1\162\1\163\1\162\4\uffff\1\141"+
        "\1\162\2\156\1\154\3\uffff\1\57\2\uffff\1\145\24\uffff\1\154\1\172"+
        "\1\154\1\164\1\141\1\165\2\151\1\166\1\172\1\144\2\172\1\163\3\uffff"+
        "\1\163\1\uffff\1\154\1\172\1\144\1\145\1\156\1\164\1\141\1\uffff"+
        "\1\172\1\uffff\1\154\1\uffff\2\145\1\172\1\uffff\2\172\1\147\1\145"+
        "\1\165\1\uffff\1\165\2\172\3\uffff\2\172\2\144\4\uffff\1\146\1\145"+
        "\1\172\1\u00a0\2\uffff";
    static final String DFA24_acceptS =
        "\4\uffff\1\6\1\7\1\uffff\1\12\2\uffff\1\16\1\17\4\uffff\1\30\1\31"+
        "\6\uffff\1\40\1\41\1\42\1\43\5\uffff\1\54\1\55\1\56\1\uffff\1\61"+
        "\1\62\1\uffff\1\2\1\1\1\3\1\4\1\5\1\11\1\10\1\64\1\14\1\13\1\15"+
        "\1\65\1\21\1\20\1\23\1\22\1\25\1\24\1\27\1\26\16\uffff\1\57\1\60"+
        "\1\63\1\uffff\1\53\7\uffff\1\45\1\uffff\1\47\1\uffff\1\52\3\uffff"+
        "\1\51\5\uffff\1\46\3\uffff\1\33\1\34\1\35\4\uffff\1\50\1\32\1\36"+
        "\1\37\4\uffff\1\44\1\66";
    static final String DFA24_specialS =
        "\2\uffff\1\0\166\uffff}>";
    static final String[] DFA24_transitionS = {
            "\2\45\2\uffff\1\45\22\uffff\1\45\1\1\1\2\1\uffff\1\42\1\uffff"+
            "\1\3\1\46\1\4\1\5\1\43\1\6\1\7\1\10\1\11\1\44\12\47\1\12\1\13"+
            "\1\14\1\15\1\16\1\17\1\uffff\32\41\1\20\1\uffff\1\21\1\uffff"+
            "\1\41\1\uffff\1\36\3\41\1\40\1\22\2\41\1\37\1\34\3\41\1\23\1"+
            "\35\2\41\1\24\1\41\1\25\1\26\1\41\1\27\3\41\1\30\1\31\1\32\1"+
            "\33\41\uffff\1\45",
            "\1\50",
            "\0\46",
            "\1\53",
            "",
            "",
            "\1\55\4\uffff\12\57",
            "",
            "\1\60\2\uffff\12\57",
            "\12\63",
            "",
            "",
            "\1\64",
            "\1\66",
            "\1\70",
            "\1\72",
            "",
            "",
            "\1\74\14\uffff\1\75",
            "\1\77\5\uffff\1\76",
            "\1\100",
            "\1\101",
            "\1\102",
            "\1\103",
            "",
            "",
            "",
            "",
            "\1\104",
            "\1\105",
            "\1\106",
            "\1\107\7\uffff\1\110",
            "\1\111",
            "",
            "",
            "",
            "\1\112\4\uffff\1\112",
            "",
            "",
            "\1\63\1\uffff\12\47\13\uffff\1\63\37\uffff\1\63",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\115",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\117",
            "\1\120",
            "\1\121",
            "\1\122",
            "\1\123",
            "\1\124",
            "\1\125",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\127",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\2\41\1\131\27\41",
            "\1\133",
            "",
            "",
            "",
            "\1\134",
            "",
            "\1\135",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\137",
            "\1\140",
            "\1\141",
            "\1\142",
            "\1\143",
            "",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "",
            "\1\145",
            "",
            "\1\146",
            "\1\147",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\153",
            "\1\154",
            "\1\155",
            "",
            "\1\156",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "",
            "",
            "",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\163",
            "\1\164",
            "",
            "",
            "",
            "",
            "\1\165",
            "\1\166",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\2\170\2\uffff\1\170\22\uffff\1\170\1\uffff\1\170\4\uffff\1"+
            "\170\170\uffff\1\170",
            "",
            ""
    };

    static final short[] DFA24_eot = DFA.unpackEncodedString(DFA24_eotS);
    static final short[] DFA24_eof = DFA.unpackEncodedString(DFA24_eofS);
    static final char[] DFA24_min = DFA.unpackEncodedStringToUnsignedChars(DFA24_minS);
    static final char[] DFA24_max = DFA.unpackEncodedStringToUnsignedChars(DFA24_maxS);
    static final short[] DFA24_accept = DFA.unpackEncodedString(DFA24_acceptS);
    static final short[] DFA24_special = DFA.unpackEncodedString(DFA24_specialS);
    static final short[][] DFA24_transition;

    static {
        int numStates = DFA24_transitionS.length;
        DFA24_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA24_transition[i] = DFA.unpackEncodedString(DFA24_transitionS[i]);
        }
    }

    class DFA24 extends DFA {

        public DFA24(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 24;
            this.eot = DFA24_eot;
            this.eof = DFA24_eof;
            this.min = DFA24_min;
            this.max = DFA24_max;
            this.accept = DFA24_accept;
            this.special = DFA24_special;
            this.transition = DFA24_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__36 | T__37 | T__38 | T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | T__45 | T__46 | T__47 | T__48 | T__49 | T__50 | T__51 | T__52 | T__53 | T__54 | T__55 | T__56 | T__57 | T__58 | T__59 | T__60 | T__61 | T__62 | T__63 | T__64 | T__65 | T__66 | T__67 | T__68 | T__69 | T__70 | JAVAUDF | OR | AND | IF | ELSE | NOT | IN | FN | ID | VAR | STAR | COMMENT | SLASH | WS | STRING | UINT | INTEGER | DECIMAL | INCLUDE );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            IntStream input = _input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA24_2 = input.LA(1);

                        s = -1;
                        if ( ((LA24_2 >= '\u0000' && LA24_2 <= '\uFFFF')) ) {s = 38;}

                        else s = 42;

                        if ( s>=0 ) return s;
                        break;
            }
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 24, _s, input);
            error(nvae);
            throw nvae;
        }

    }
 

}