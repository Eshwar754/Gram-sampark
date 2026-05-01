# Gram-Sampark – Offline-First Rural Health Records

This is a complete working project demonstrating an offline-first patient record system tailored for rural health workers operating in areas with limited or no internet connectivity. 

## 1. System Architecture Explanation

The application is built as a **Progressive Web App (PWA)** combined with **Firebase Firestore** as the backend database.
- **Frontend**: Vanilla JavaScript, HTML5, CSS3. We avoid heavy frameworks to keep the app lightweight and fast.
- **Service Worker**: Caches the application's static assets (`index.html`, `style.css`, `app.js`, `manifest.json`) using the Cache API. This ensures the app shell loads even when entirely offline, fulfilling the PWA requirements.
- **Database / Offline Persistence**: We use Firebase's Firestore modular SDK (`v10`). Specifically, we call `enableIndexedDbPersistence()` which allows the app to cache queried documents and queue local writes.
- **Conflict Handling / LWW**: Firestore natively uses a Last Write Wins strategy based on server timestamps (`serverTimestamp()`). We also explicitly added a manual client-side check (`client_timestamp`) before updating existing records to prevent overwriting newer server data with older offline data if a collision occurs.

## 2. Folder Structure

```
gram-sampark/
│ 
├── index.html        # Main HTML structure and UI
├── style.css         # Styling for the application
├── app.js            # Main application logic and Firebase integration
├── sw.js             # Service Worker for PWA caching
├── manifest.json     # PWA manifest for installability
└── README.md         # Documentation and instructions
```

## 3. Source Code
The source code has already been generated in this folder: `index.html`, `style.css`, `app.js`, `sw.js`, and `manifest.json`.

## 4. Firebase Configuration Steps

To link this code to your own Firebase project:
1. Go to the [Firebase Console](https://console.firebase.google.com/).
2. Click **Create a project** and name it "Gram-Sampark".
3. Inside your project, click the **Web** icon (</>) to add a Firebase app.
4. Register the app (you can check "Also set up Firebase Hosting" if you wish).
5. Copy the `firebaseConfig` object provided.
6. Open `app.js` and replace the `firebaseConfig` object at the top of the file with your own credentials.
7. Go to **Build > Firestore Database** in the left menu and click **Create database**.
8. Start in **Test mode** (or set up basic security rules allowing reads/writes).
9. Make sure the database location is suitable for your region.

## 5. Deployment Guide (Firebase Hosting)

To deploy the app so it's globally accessible:
1. Ensure you have Node.js installed.
2. Install the Firebase CLI globally: 
   ```bash
   npm install -g firebase-tools
   ```
3. Login to Firebase from your terminal:
   ```bash
   firebase login
   ```
4. Initialize your project inside the `gram-sampark` folder:
   ```bash
   firebase init
   ```
   - Select **Hosting: Configure files for Firebase Hosting**.
   - Select **Use an existing project** and choose your "Gram-Sampark" project.
   - For the public directory, type `.`, or put your files into a `public` folder and select that.
   - Configure as a single-page app: **No**.
   - Set up automatic builds: **No**.
5. Deploy the application:
   ```bash
   firebase deploy
   ```

## 6. Offline Testing Instructions

Using Chrome / Edge DevTools:
1. Serve the app locally (e.g., using `npx serve` or simply opening it if you deploy to Firebase). It needs to be on an `http://localhost` or `https://` domain for the Service Worker to register.
2. Open **Developer Tools** (F12 or Ctrl+Shift+I).
3. Go to the **Application** tab. Check the **Service Workers** section to ensure `sw.js` is activated.
4. Go to the **Network** tab, check the **Offline** box (or select "Offline" from the throttling dropdown).
5. Add a new patient record through the form. 
   - You will see the status indicator turn red ("Offline").
   - The UI will say "Pending..." for the timestamp since it hasn't reached the server, but the record will appear in the list!
6. Uncheck the **Offline** box.
   - The status indicator will turn green ("Online & Syncing").
   - Firestore will automatically synchronize the queued write in the background.
   - The timestamp will eventually update to the actual server time. 
7. Refresh the page while still offline to confirm the UI loads instantly from cache and the patient records are pulled from the local IndexedDB.
