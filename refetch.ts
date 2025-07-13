import { Page } from "puppeteer";

export async function refetch(page: Page) {
  // Navigate the page to a URL
  await page.goto("https://ventscape.life/");
  await page.content();
  const indexedDB = await page.evaluate(
    async (): Promise<{
      token?: string;
    }> => {
      try {
        const db = await new Promise<IDBDatabase>((resolve, reject) => {
          // @ts-ignore
          const request = indexedDB.open("firebaseLocalStorageDb");
          request.onsuccess = (event) => resolve(event.target.result);
          request.onerror = (event) => reject(event.target.error);
        });
        const objectStore = db
          .transaction(["firebaseLocalStorage"], "readonly")
          .objectStore("firebaseLocalStorage");
        const data = await new Promise<any[]>((resolve, reject) => {
          const request = objectStore.getAll();
          request.onsuccess = (event: any) => resolve(event.target.result);
          request.onerror = (event: any) => reject(event.target.error);
        });
        return {
          token: data[0].value.stsTokenManager.accessToken,
        };
      } catch (errorMessage) {
        throw new Error("Failed to get token" + errorMessage);
      }
    }
  );
  const { token } = indexedDB;
  if (!token || token === "") {
    throw new Error("Failed to get token");
  }
  await page.setExtraHTTPHeaders({
    Authorization: `Bearer ${token}`,
  });
  await page.goto("https://ventscape.herokuapp.com/messages?language=en");
  await page.content();

  const oldMessages = await page.evaluate(() => {
    // @ts-ignore
    return JSON.parse(document.querySelector("body").innerText);
  });
  return oldMessages as {
    messages: {
      id: string;
      messageText: string;
      color: string;
      userId: string;
      createdAt: string;
      nickname: string | null;
      font: string | null;
    }[];
  };
}
