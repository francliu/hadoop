
import java.awt.Font;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

public class BarChart {
	ChartPanel frame1;
	public  BarChart(){
		CategoryDataset dataset = getDataSet();
        JFreeChart chart = ChartFactory.createBarChart3D(
       		                 "ˮ��", // ͼ�����
                            "ˮ������", // Ŀ¼�����ʾ��ǩ
                            "����", // ��ֵ�����ʾ��ǩ
                            dataset, // ���ݼ�
                            PlotOrientation.VERTICAL, // ͼ����ˮƽ����ֱ
                            true,           // �Ƿ���ʾͼ��(���ڼ򵥵���״ͼ������false)
                            false,          // �Ƿ����ɹ���
                            false           // �Ƿ�����URL����
                            );
        
        //�����￪ʼ
        CategoryPlot plot=chart.getCategoryPlot();//��ȡͼ���������
        CategoryAxis domainAxis=plot.getDomainAxis();         //ˮƽ�ײ��б�
         domainAxis.setLabelFont(new Font("����",Font.BOLD,14));         //ˮƽ�ײ�����
         domainAxis.setTickLabelFont(new Font("����",Font.BOLD,12));  //��ֱ����
         ValueAxis rangeAxis=plot.getRangeAxis();//��ȡ��״
         rangeAxis.setLabelFont(new Font("����",Font.BOLD,15));
          chart.getLegend().setItemFont(new Font("����", Font.BOLD, 15));
          chart.getTitle().setFont(new Font("����",Font.BOLD,20));//���ñ�������
          
          //�������������Ȼ�����е�࣬��ֻΪһ��Ŀ�ģ����������������
          
         frame1=new ChartPanel(chart,true);        //����Ҳ������chartFrame,����ֱ������һ��������Frame
         
	}
	   private static CategoryDataset getDataSet() {
           DefaultCategoryDataset dataset = new DefaultCategoryDataset();
           dataset.addValue(100, "����", "ƻ��");
           dataset.addValue(100, "�Ϻ�", "ƻ��");
           dataset.addValue(100, "����", "ƻ��");
           dataset.addValue(200, "����", "����");
           dataset.addValue(200, "�Ϻ�", "����");
           dataset.addValue(200, "����", "����");
           dataset.addValue(300, "����", "����");
           dataset.addValue(300, "�Ϻ�", "����");
           dataset.addValue(300, "����", "����");
           dataset.addValue(400, "����", "�㽶");
           dataset.addValue(400, "�Ϻ�", "�㽶");
           dataset.addValue(400, "����", "�㽶");
           dataset.addValue(500, "����", "��֦");
           dataset.addValue(500, "�Ϻ�", "��֦");
           dataset.addValue(500, "����", "��֦");
           return dataset;
}
public ChartPanel getChartPanel(){
	return frame1;
	
}
}

