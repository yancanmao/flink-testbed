package flinkapp.wordcount;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class test {
    private static Boolean isErrorHappened() throws IOException {
        Scanner scanner = new Scanner(new File("/home/myc/workspace/flink-related/flink-testbed/src/main/resources/err.txt"));
        String line = scanner.nextLine();
        if(line.equals("1")) {
            return true;
        } else {
            // modify and return false
            System.out.println(line);
            try {
                FileWriter fileWriter = new FileWriter(new File(
                        "/home/myc/workspace/flink-related/flink-testbed/src/main/resources/err.txt"), false);
                fileWriter.write("1");
                fileWriter.close();
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
            return false;
        }
    }

    static String str = "b71731f1c0df9c3076c4a455334d0ad6,Count -> Latency Sink-1,1,100.00725515250242,0.0,149.81156029086998,0.0,79498.78,10211,0,1.498006920222141,0,0:75&1:79&2:75&3:73&4:90&5:82&6:70&7:78&8:71&9:66&10:67&11:88&12:77&13:78&14:73&15:68&16:69&17:61&18:66&19:80&20:87&21:73&22:76&23:68&24:69&25:77&26:82&27:81&28:101&29:78&30:83&31:74&32:79&33:80&34:84&35:75&36:85&37:86&38:78&39:81&40:95&41:75&42:83&43:87&44:72&45:95&46:75&47:70&48:76&49:81&50:76&51:75&52:79&53:79&54:79&55:74&56:88&57:73&58:75&59:101&60:94&61:77&62:84&63:79&64:82&65:89&66:213&67:94&68:79&69:83&70:63&71:70&72:71&73:89&74:67&75:73&76:78&77:84&78:89&79:77&80:64&81:82&82:68&83:66&84:66&85:81&86:67&87:91&88:75&89:80&90:84&91:83&92:68&93:70&94:72&95:81&96:65&97:83&98:85&99:87&100:79&101:86&102:88&103:68&104:72&105:80&106:84&107:83&108:81&109:86&110:79&111:77&112:83&113:86&114:87&115:93&116:70&117:89&118:84&119:71&120:69&121:74&122:83&123:73&124:74&125:92&126:84&127:92";
    public static void main(String[] args) throws IOException {
        System.out.println(isErrorHappened());
        System.out.println(isErrorHappened());
    }
}
