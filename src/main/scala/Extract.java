import java.io.*;

public class Extract {
    public static void main(String args[]) throws IOException {
        BufferedReader reader= new BufferedReader(new FileReader("result_ttp_1000.txt"));
        BufferedWriter writer=new BufferedWriter(new FileWriter("extract2.txt"));

        String line="";
        while((line = reader.readLine()) != null) {
            //System.out.println(line);
            System.out.print(line.split(" ")[8]+"\n");
            writer.write(line.split(" ")[8] + "\n");
        }

        int cnt= 1;
        /*while(cnt <= 300)
        {
            writer.write("Window"+cnt+"\n");
            cnt++;
        }*/

        writer.close();
    }
}
