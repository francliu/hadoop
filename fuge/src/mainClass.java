
import java.awt.GridLayout;

import javax.swing.JFrame;

public class mainClass {
public static void main(String args[]){
	JFrame frame=new JFrame("Java����ͳ��ͼ");
	frame.setLayout(new GridLayout(2,2,10,10));
	frame.add(new BarChart().getChartPanel());           //�������ͼ
	frame.add(new BarChart1().getChartPanel());          //�������ͼ����һ��Ч��
	frame.add(new PieChart().getChartPanel());           //��ӱ�״ͼ
	frame.add(new TimeSeriesChart().getChartPanel());    //�������ͼ
	frame.setBounds(50, 50, 800, 600);
	frame.setVisible(true);
}
}
