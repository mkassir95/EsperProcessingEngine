package org.example;

import com.jcraft.jsch.*;

import java.io.InputStream;

public class SSHUtil {
    private String host;
    private String username;
    private String password;

    public SSHUtil(String host, String username, String password) {
        this.host = host;
        this.username = username;
        this.password = password;
    }

    public String executeCommand(String command) {
        String output = "";
        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(username, host, 22);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(password);
            session.connect();

            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            InputStream in = channel.getInputStream();
            channel.connect();

            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    output += new String(tmp, 0, i);
                }
                if (channel.isClosed()) {
                    if (in.available() > 0) continue;
                    System.out.println("exit-status: " + channel.getExitStatus());
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {}
            }
            channel.disconnect();
            session.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }
}
