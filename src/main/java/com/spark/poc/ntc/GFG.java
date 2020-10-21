package com.spark.poc.ntc;

//Java program to check if a string is 'n'
//times repetition of one of its substrings
import java.io.*;

class GFG {

	// A utility function to fill lps[] or compute
	// prefix funcrion used in KMP string matching
	// algorithm. Refer
	// https://www.geeksforgeeks.org/archives/11902
	// for details
	static void computeLPSArray(String str, int M, int lps[]) {
		// lenght of the previous
		// longest prefix suffix
		int len = 0;

		int i;

		lps[0] = 0; // lps[0] is always 0
		i = 1;

		// the loop calculates lps[i]
		// for i = 1 to M-1
		while (i < M) {
			if (str.charAt(i) == str.charAt(len)) {
				len++;
				lps[i] = len;
				i++;
			} else // (pat[i] != pat[len])
			{
				if (len != 0) {
					// This is tricky. Consider the
					// example AAACAAAA and i = 7.
					len = lps[len - 1];

					// Also, note that we do
					// not increment i here
				} else // if (len == 0)
				{
					lps[i] = 0;
					i++;
				}
			}
		}
	}

	// Returns true if str is repetition of
	// one of its substrings else return false.
	static boolean isRepeat(String str) {
		// Find length of string and create
		// an array to store lps values used in KMP
		int n = str.length();
		int lps[] = new int[n];

		// Preprocess the pattern (calculate lps[] array)
		computeLPSArray(str, n, lps);

		// Find length of longest suffix
		// which is also prefix of str.
		int len = lps[n - 1];

		// If there exist a suffix which is also
		// prefix AND Length of the remaining substring
		// divides total length, then str[0..n-len-1]
		// is the substring that repeats n/(n-len)
		// times (Readers can print substring and
		// value of n/(n-len) for more clarity.
		return (len > 0 && n % (n - len) == 0) ? true : false;
	}

	// Driver program to test above function
	public static void main(String[] args) {
		String txt[] = { "ABCABC"};
		//, "ABABAB", "ABCDABCD", "GEEKSFORGEEKS","GEEKGEEK", "AAAACAAAAC", "ABCDABC" 
		int n = txt.length;
		for (int i = 0; i < n; i++) {
			if (isRepeat(txt[i]) == true)
				System.out.println("True");
			else
				System.out.println("False");
		}
	}
}

// This code is contributed by Prerna Saini

