from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd

def main():
    documents = ["A 4 year old Malayan tiger who had a dry cough and a slight loss of appetite tested positive "
                 "for the virus that has caused a human pandemic the Bronx Zoo reported on Sunday",
                 "As the coronavirus spreads and locked down communities stock up products needed by "
                 "allergy sufferers are increasingly hard to come by",
                 "In the city where the coronavirus outbreak was first reported the reopening of "
                 "outbound travel won’t end hard times wariness or confinement",
                 "Formula One teams are producing CPAP machines for patients while several sporting "
                 "goods businesses are making personal protective equipment for health care workers",
                 "The news was the latest setback in the ships troubled mission to New York",
                 "The trajectory of the coronavirus pandemic varies widely from country to country",
                 "The number of new cases each day appears to be falling in some nations",
                 "Scientists are testing everyday items to find the best protection from coronavirus",
                 "Federal health officials have now recommended that we cover our faces with fabric "
                 "during the coronavirus pandemic"]

    vectorizer = CountVectorizer()
    matrix = vectorizer.fit_transform(documents)
    from sklearn.cluster import KMeans
    km = KMeans(n_clusters=2).fit(matrix)

    feature_names = vectorizer.get_feature_names()
    ordered_centroids = km.cluster_centers_.argsort()[:, ::-1]

    for cluster_num in range(2):
        print('CLUSTER #' +str(cluster_num+1))
        feature_list = list()
        for i in ordered_centroids[cluster_num, :10]:
            feature_list.append(feature_names[i])
        print(feature_list)



if __name__ == "__main__":
    main()