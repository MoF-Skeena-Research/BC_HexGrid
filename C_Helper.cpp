#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
IntegerVector unlist_sgbp(Rcpp::List L) {
  IntegerVector out = IntegerVector(L.size());
  NumericVector temp;
  for(int i = 0; i < L.size(); i++){
    temp = L[i];
    if(temp[0] < 0){
      out[i] = NA_INTEGER;
    }else{
      out[i] = temp[0];
    }
  }
  return (out);
}
