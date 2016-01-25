
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;

import Jama.Matrix;

public class SimpleModularity2 {

	public static DecimalFormat myformat = new DecimalFormat("#0");
	public static DecimalFormat myformat2 = new DecimalFormat("#0.0000 ");
	public static int nodesNum, edgesNum;
	public static int computeKmeansTimes = 0;
	public static double[] ED;
	public static Boolean update = false;
	public static int groupsNum = 3;
	public static Matrix B;

	/**
	 * @param args
	 * @throws FileNotFoundException
	 */
	public static void print(double[][] A, int n1, int n2) {

		for (int i = 0; i < n1; i++) {
			for (int j = 0; j < n2; j++) {
				System.out.print(myformat2.format(A[i][j]) + " ");
			}
			System.out.println();
		}
	}

	public static double[][] readFileToInitializeAdjacentArray(File file)
			throws IOException {
		double[][] A;
		BufferedReader reader = null;
		String tmpStr = null;
		String[] strLine = null;
		reader = new BufferedReader(new FileReader(file));
		tmpStr = reader.readLine();
		strLine = tmpStr.split(" ");
		nodesNum = Integer.parseInt(strLine[0]);
		int n = nodesNum;
		edgesNum = Integer.parseInt(strLine[1]);
		A = new double[n][n];
		ED = new double[n];

		while ((tmpStr = reader.readLine()) != null) {
			strLine = tmpStr.split(" |,");
			int i = Integer.parseInt(strLine[0]);
			int j = Integer.parseInt(strLine[1]);
			 A[i][j] = 1;
			 A[j][i] = 1;
		}
		reader.close();
		return A;
	}

	public static double[] computeDegree(double[][] A) {
		int n = A[0].length;
		double[] degree = new double[n];
		int k = 0;
		for (int i = 0; i < n; i++) {
			double temSum = 0;

			for (int j = 0; j < n; j++) {
				temSum = temSum + A[i][j];
			}
			degree[k] = temSum;
			k++;
		}
		for (int i = 0; i < n; i++) {
			System.out.print(myformat.format(degree[i]) + " ");
		}
		System.out.println();
		return degree;
	}
	public static double[] computeBMatrixEigenvalues(double[][] array) {
		double[] degree = computeDegree(array);
		Matrix d = new Matrix(degree, nodesNum);
		Matrix Tmp;
		Tmp = d.times(d.transpose());//
		Tmp = Tmp.times(1.0 / (2 * edgesNum));

		Matrix A = new Matrix(array);
		B = A.minus(Tmp);// B = A - 1/2m * d * dT
		int n = array[0].length;

		for (int i = 0; i < n; i++) {
			ED[i] = B.eig().getD().get(i, i);

			System.out.print(myformat2.format(ED[i]) + " ");
		}
		System.out.println();
		double[] maxEigenvector = new double[nodesNum];
		for (int i = 0; i < n; i++) {
			maxEigenvector[i] = B.eig().getV().get(i, nodesNum - 1);
		}
		 B.eig().getV().print(5, 2);
		return maxEigenvector;
	}

	public static int[]  findSmallGroups(double[] maxEigenVector) {
		update = false;

		int n = nodesNum;
		double[] tmp = maxEigenVector;
		int[] nodesIndex = new int[n];
		for (int i = 0; i < n; i++)
			nodesIndex[i] = i + 1;
		// for(int k =0;k<=4;k++){
		int k = groupsNum;
		System.out.println();

		double[] centers = new double[k];
		centers[0] = tmp[0];
		centers[1] = tmp[1];
		centers[2] = tmp[2];
		
		int[] tagArr = new int[nodesNum];
		double[][] distance = new double[nodesNum][k];

		computeKmeans(centers, tmp, tagArr, distance, k);

		for (int i = 0; i < n; i++) {
			System.out.print(tagArr[i] + " ");

		}
		System.out.println();

		while (update == true) {
			update = false;
			
			double center = 0;int centerNum = 0;
			int centersRow = 0;
			int tag = 0;
			while (centersRow < groupsNum) {
				center = 0;
				centerNum = 0;

				for (int i = 0; i < nodesNum; i++) {
					if (tagArr[i] == tag) {
						center = center + tmp[i];
						centerNum++;
					}
				}
				centers[centersRow] = center / centerNum;
				tag++;
				centersRow++;
			}	
			
			computeKmeans(centers, tmp, tagArr, distance, k);
			for (int i = 0; i < n; i++) {
				System.out.print(tagArr[i] + " ");
			}
			System.out.println();
		}
		return tagArr;
	}

	public static int findMinDis(double[] d) {
		int minIndex = 0;
		double min = d[0];
		for (int i = 1; i < d.length; i++) {
			if (d[i] < min) {
				min = d[i];
				minIndex = i;
			}
		}

		return minIndex;
	}

	public static void computeKmeans(double[] centers, double[] tmp,
			int[] tagArr, double[][] distance, int k) {
		
		
		for(int i=0;i<groupsNum;i++){
			System.out.print(myformat2.format(centers[i])+ " ");
		}
		System.out.println();
		computeKmeansTimes++;
		for (int i = 0; i < nodesNum; i++) {
			int centerRow = 0;
			while (centerRow < k) {
				double dis = 0;
				dis = Math.pow(tmp[i] - centers[centerRow], 2);
				distance[i][centerRow] = Math.sqrt(dis);
				centerRow++;
			}
		}
		

		for (int i = 0; i < nodesNum; i++) {
			int tag = findMinDis(distance[i]);
			if (tagArr[i] != tag) {
				tagArr[i] = tag;
				update = true;
			}
		}
	}
	public static double[][] constructXMatrix(int[] tagarr){
		System.out.println();
		double[][] arrayX = new double[nodesNum][groupsNum];
		for(int i =0;i<nodesNum;i++){
			arrayX[i][tagarr[i]] = 1;
		}
		for(int i =0;i<nodesNum;i++){
			for(int j=0;j<groupsNum;j++){
				System.out.print(myformat.format(arrayX[i][j])+" ");
			}
			System.out.println();
		}
		return arrayX;
	}
	
	public static double computeQvalue(double[][] arrayX){
		//double[] Qvalue = new double[groupsNum];
		int m = edgesNum;
		Matrix X = new Matrix(arrayX);
		Matrix XT = X.transpose();
		Matrix Tmp = XT.times(B);//Tmp = XT*B
		Tmp = Tmp.times(X);//Tmp = XT*B*X
		double Q = 1.0/(2*m)*Tmp.trace();//Tmp = 1/(2m)  *  Tr(XT*B*X)
		System.out.println("Q="+Q);
		return Q;
	}
	
	public static void output(double[][] arrayX){
		//String outPutStr = "";
		for(int j=0;j<groupsNum;j++){
			String outPutStr = "";
			System.out.print("group"+j+":");
			for(int i=0;i<nodesNum;i++){
				if(arrayX[i][j]>0.0){
					outPutStr = outPutStr+i+",";
				}
			}
			System.out.println(outPutStr);
		}
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		File file = new File(args[0]);
		double[][] A = readFileToInitializeAdjacentArray(file);
		System.out.println("ok");
		double[] maxEigenVector = computeBMatrixEigenvalues(A);
		int[] tagarr = findSmallGroups(maxEigenVector);
		double[][] arrayX = constructXMatrix(tagarr);
		computeQvalue(arrayX);
		output(arrayX);
	}
}
