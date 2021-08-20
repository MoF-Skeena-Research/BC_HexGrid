#include <Rcpp.h>
#include <stdio.h>
using namespace Rcpp;
using namespace std;
// [[Rcpp::plugins("cpp17")]]

// [[Rcpp::export]]
DataFrame unlist_sgbp(Rcpp::List L) {
  vector<int> values;
  vector<int> index;
  NumericVector temp;
  for(int i = 0; i < L.size(); i++){
    Rcpp::checkUserInterrupt();
    temp = L[i];
    if(temp[0] < 0){
      values.push_back(-1);
      index.push_back(i);
    }else{
      for(int j = 0; j < temp.size(); j++){
        values.push_back(temp[j]);
        index.push_back(i);
      }
    }
  }
  DataFrame out = DataFrame::create(_["Index"] = index, _["OldIdx"] = values);
  return(out);
}
