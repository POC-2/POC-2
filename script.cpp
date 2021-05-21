#include<bits/stdc++.h>
#define ll long long
#define F first
#define S second
using namespace std;
int main(){
    ios_base::sync_with_stdio(false);
	cin.tie(NULL);
	cout.tie(NULL);
    for(int i=1;i<=500;i++){
        cout<<"{ \"index\": { \"_id\": "<<i<<" }}"<<"\n";
        cout<<"{\"business_address\":\""<<(315+i)<<" California St\",\"business_city\":\"San Francisco\",\"business_id\":\""<<(24936+i)<<"\""<<",\"business_longitude\":\""<<(-122.4+(double)i/10)<<"\",\"business_name\":\"San Francisco Soup Company"<<i<<"\",\"business_postal_code\":\""<<(94104+i)<<"\",\"business_state\":\"CA\",\"inspection_date\":\"2016-06-09T00:00:00+07:00\",\"inspection_id\":\""<<i<<"\",\"inspection_score\":"<<(77+i)<<",\"inspection_type\":\"Routine - Unscheduled\",\"risk_category\":\"Low Risk\",\"violation_description\":\"Improper food labeling or menu misrepresentation\",\"violation_id\":\""<<i<<"\"}"<<"\n";

    }
    return 0;
}



