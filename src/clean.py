from .utils import logger_util
import pandas as pd
from bs4 import BeautifulSoup
from dataclasses import dataclass
from io import BytesIO

logger = logger_util(__name__)


@dataclass
class Clean:

    # this might need to be a stream? or azure blob storage?
    
    def create_csv_dataframe_from_stream(self, stream: bytes) -> pd.DataFrame:
        
        df = pd.read_csv(BytesIO(stream))
        
        return df
    
    def standards_dataframe(self, csv_file_stream: bytes) -> pd.DataFrame:

        dataframe = pd.read_csv(BytesIO(csv_file_stream))

        dataframe["Name"] = dataframe["standard_title"].str.extract("Part (\d+) -")
        dataframe["Content"] = (
            dataframe["standard_page_title"] + "\n" + dataframe["standard_content"]
        )
        final_df = dataframe[["Name", "Content"]]

        return final_df


    def create_bs4_object_from_series(self, df: pd.Series) -> BeautifulSoup:

        df = df.apply(lambda x: BeautifulSoup(x, "html.parser"))
        return df        
    
    
    def get_article_from_html(self, file: bytes) -> pd.DataFrame:

        df = pd.read_csv(BytesIO(file))
        articles = df["article"].apply(lambda x: BeautifulSoup(x, "html.parser"))
        df["ExternalId"] = articles.apply(
            lambda x: x.find("article").get("data-history-node-id")
        )

        df = df.rename(columns={"article": "content"})
        return df

    # strip title column
    def strip_title(self, df: pd.DataFrame):

        df1 = df.copy()
        df1["title"] = df1["title"].str.split("- \[\d", expand=True)[0]
        df1["title"] = df1["title"].str.strip()  # remove whitespace
        df1["content"] = df1["content"] + df1["article_title"]

        df1 = df1.drop("article_title", axis=1)

        return df1

    # create citation dataframe
    def citation_df(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Explodes along the title column after splitting out citations.
        joins and uses externalId for many:1 join"""

        citation_df = (
            dataframe[["ExternalId"]]
            .join(
                dataframe["title"]
                .str.split("- \[", expand=True)[1]
                .str.replace("\[|\]", "", regex=True)
                .str.split(";")
                .explode()
            )
            .reset_index(drop=True)
        )

        return citation_df.rename(columns={1: "Content"})


    def remove_ul_header(self, soup: pd.Series) -> BeautifulSoup:
        
        for node in soup.findAll("ul", {"class": "bulleted-list-header node-header"}):
            node.decompose()

        return soup.prettify()


    def remove_copyright_header(self, soup : pd.Series) -> BeautifulSoup:
        
        for node in soup.findAll("section", {"class": "block clearfix"}):
            node.decompose()

        return soup.prettify()