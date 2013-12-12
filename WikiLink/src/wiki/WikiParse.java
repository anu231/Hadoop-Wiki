package wiki;

import net.java.textilej.parser.MarkupParser;
import net.java.textilej.parser.builder.HtmlDocumentBuilder;
import net.java.textilej.parser.markup.mediawiki.MediaWikiDialect;

import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.BufferedReader;
import java.io.FileReader;

public class WikiParse {

	public String parseWikiMarkup(String markup){
		StringWriter writer = new StringWriter();

        HtmlDocumentBuilder builder = new HtmlDocumentBuilder(writer);
        builder.setEmitAsDocument(false);

        MarkupParser parser = new MarkupParser(new MediaWikiDialect());
        parser.setBuilder(builder);
        parser.parse(markup);

        final String html = writer.toString();
        return html;
	}
}
