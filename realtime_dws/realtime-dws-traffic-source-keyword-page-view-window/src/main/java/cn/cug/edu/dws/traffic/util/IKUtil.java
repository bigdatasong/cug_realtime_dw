package cn.cug.edu.dws.traffic.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * author song
 * date 2025-01-16 19:03
 * Desc
 */
public class IKUtil {


    /*
        只能切词，没有NLP(自然语言处理)功能
     */
    public static void main(String[] args) {

        // System.out.println(splitWord("我喜欢抽烟喝酒烫头洗屁股眼!", false));

    }
    public static Set<String> splitWord(String str, boolean useSmart){

        Set<String> words = new HashSet<>();
        /*
            IKSegmenter : 切词器的核心api
            IKSegmenter(
                Reader input,  把字符串变为字符流
                boolean useSmart, 是否是智能切词，
                        true，智能切词
                        false, 最大化切词(切出很 无语的词 )
                 )
         */
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(str), useSmart);

        try {
            Lexeme lexeme = ikSegmenter.next();
            while (lexeme != null){
                String lexemeText = lexeme.getLexemeText();
                //把切的内容加入到集合
                words.add(lexemeText);
                //继续切词
                lexeme = ikSegmenter.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return words;
    }
}
